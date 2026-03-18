import io
import json
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from charset_normalizer import from_bytes
from flask import Flask, jsonify, render_template, request, send_file

BASE_DIR = Path(__file__).resolve().parent
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
    static_url_path="/static",
)

MAX_PREVIEW_ROWS = 50
MAX_MEMORY_DATASETS = 8


@dataclass
class DatasetEntry:
    df: pd.DataFrame
    filename: str


_DATASETS: Dict[str, DatasetEntry] = {}


def _evict_if_needed():
    if len(_DATASETS) <= MAX_MEMORY_DATASETS:
        return
    keys = list(_DATASETS.keys())
    for k in keys[: max(0, len(keys) - MAX_MEMORY_DATASETS)]:
        _DATASETS.pop(k, None)


def _best_effort_decode(raw: bytes) -> Tuple[str, str]:
    for enc in ["utf-8-sig", "utf-8", "gb18030", "gbk"]:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            pass

    best = from_bytes(raw).best()
    if best is not None:
        return str(best), best.encoding or "detected"

    return raw.decode("utf-8", errors="replace"), "utf-8(replace)"


def _parse_csv(raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, enc = _best_effort_decode(raw)
    buf = io.StringIO(text)
    df = pd.read_csv(buf, sep=None, engine="python")
    meta = {"format": "csv", "encoding": enc, "delimiter": "auto"}
    return df, meta


def _parse_json(raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, enc = _best_effort_decode(raw)
    obj = json.loads(text)

    if isinstance(obj, list):
        df = pd.json_normalize(obj)
        meta = {"format": "json", "encoding": enc, "json_shape": "array"}
        return df, meta

    if isinstance(obj, dict):
        try:
            df_try = pd.DataFrame(obj)
            if len(df_try.columns) > 0 and len(df_try) > 1:
                meta = {"format": "json", "encoding": enc, "json_shape": "object(table-like)"}
                return df_try, meta
        except Exception:
            pass

        df = pd.json_normalize(obj)
        meta = {"format": "json", "encoding": enc, "json_shape": "object"}
        return df, meta

    raise ValueError("Unsupported JSON root type (must be array or object).")


def _parse_jsonl(raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, enc = _best_effort_decode(raw)
    rows = []
    for i, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            rows.append(json.loads(s))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSONL at line {i}: {e}") from e

    df = pd.json_normalize(rows)
    meta = {"format": "jsonl", "encoding": enc, "lines": len(rows)}
    return df, meta


def _parse_txt(raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, enc = _best_effort_decode(raw)
    lines = [ln.rstrip("\n\r") for ln in text.splitlines()]
    df = pd.DataFrame({"text": lines})
    meta = {"format": "txt", "encoding": enc, "lines": len(lines)}
    return df, meta


def _parse_xlsx(raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    buf = io.BytesIO(raw)
    df = pd.read_excel(buf, engine="openpyxl")
    meta = {"format": "xlsx", "sheet": "first"}
    return df, meta


def _infer_semantic_type(series: pd.Series) -> str:
    s = series
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    if pd.api.types.is_numeric_dtype(s):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"

    non_na = s.dropna()
    if len(non_na) == 0:
        return "empty"

    num = pd.to_numeric(non_na, errors="coerce")
    numeric_ratio = float(num.notna().mean())
    if numeric_ratio >= 0.9:
        return "numeric"

    dt = pd.to_datetime(non_na, errors="coerce", utc=False)
    dt_ratio = float(dt.notna().mean())
    if dt_ratio >= 0.9:
        return "datetime"

    return "text"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (np.floating, float)):
            if math.isfinite(float(x)):
                return float(x)
            return None
        if isinstance(x, (np.integer, int)):
            return float(x)
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _column_profile(df: pd.DataFrame) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for col in df.columns:
        s = df[col]
        inferred = _infer_semantic_type(s)
        missing = int(s.isna().sum())
        unique = int(s.nunique(dropna=True))

        item: Dict[str, Any] = {
            "name": str(col),
            "inferred_type": inferred,
            "missing": missing,
            "unique": unique,
        }

        if inferred == "numeric":
            num = pd.to_numeric(s, errors="coerce")
            num_non_na = num.dropna()
            if len(num_non_na) == 0:
                item["numeric_stats"] = None
            else:
                q = num_non_na.quantile([0.0, 0.25, 0.5, 0.75, 1.0]).to_dict()
                item["numeric_stats"] = {
                    "min": _safe_float(q.get(0.0)),
                    "q25": _safe_float(q.get(0.25)),
                    "median": _safe_float(q.get(0.5)),
                    "q75": _safe_float(q.get(0.75)),
                    "max": _safe_float(q.get(1.0)),
                    "mean": _safe_float(num_non_na.mean()),
                    "std": _safe_float(num_non_na.std(ddof=1) if len(num_non_na) > 1 else 0.0),
                }

        if inferred == "text":
            text = s.astype("string")
            lengths = text.dropna().map(lambda x: len(str(x)))
            if len(lengths) == 0:
                item["text_stats"] = None
            else:
                item["text_stats"] = {
                    "avg_len": _safe_float(lengths.mean()),
                    "max_len": int(lengths.max()),
                }

        out.append(item)

    return out


def _preview(df: pd.DataFrame) -> Dict[str, Any]:
    head = df.head(MAX_PREVIEW_ROWS)
    return {
        "columns": [str(c) for c in head.columns],
        "rows": head.replace({np.nan: None}).to_dict(orient="records"),
        "total_rows": int(len(df)),
        "total_cols": int(df.shape[1]),
        "preview_rows": int(len(head)),
    }


def _parse_by_extension(filename: str, raw: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return _parse_csv(raw)
    if lower.endswith(".json"):
        return _parse_json(raw)
    if lower.endswith(".jsonl"):
        return _parse_jsonl(raw)
    if lower.endswith(".txt"):
        return _parse_txt(raw)
    if lower.endswith(".xlsx"):
        return _parse_xlsx(raw)

    raise ValueError("Unsupported file type. Please upload csv/json/jsonl/txt/xlsx.")


def _get_dataset(dataset_id: str) -> DatasetEntry:
    entry = _DATASETS.get(dataset_id)
    if entry is None:
        raise KeyError("dataset_not_found")
    return entry


def _normalize_columns_arg(columns: Any, df: pd.DataFrame) -> List[str]:
    if columns is None:
        return [str(c) for c in df.columns]
    if not isinstance(columns, list):
        raise ValueError("columns must be a list or null")
    cols = []
    df_cols = df.columns.astype(str).tolist()
    for c in columns:
        cs = str(c)
        if cs in df_cols:
            cols.append(cs)
    return cols


def _coerce_to_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    df2 = df.copy()
    for c in columns:
        if c in df2.columns:
            df2[c] = pd.to_numeric(df2[c], errors="coerce")
    return df2


def _apply_cleaning(df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    df2 = df.copy()
    cols_default = _normalize_columns_arg(spec.get("columns"), df2)
    ops = spec.get("ops") or {}

    if ops.get("drop_duplicates"):
        subset = cols_default if len(cols_default) > 0 else None
        df2 = df2.drop_duplicates(subset=subset)

    if ops.get("drop_na_rows"):
        subset = cols_default if len(cols_default) > 0 else None
        df2 = df2.dropna(subset=subset)

    fill_na = ops.get("fill_na")
    if isinstance(fill_na, dict):
        mode = fill_na.get("mode")
        value = fill_na.get("value")
        cols = _normalize_columns_arg(fill_na.get("columns"), df2) if fill_na.get("columns") is not None else cols_default

        if mode in ("mean", "median"):
            df2 = _coerce_to_numeric(df2, cols)
            for c in cols:
                if c not in df2.columns:
                    continue
                if mode == "mean":
                    v = df2[c].mean(skipna=True)
                else:
                    v = df2[c].median(skipna=True)
                if pd.notna(v):
                    df2[c] = df2[c].fillna(v)
        elif mode == "value":
            for c in cols:
                if c in df2.columns:
                    df2[c] = df2[c].fillna(value)
        else:
            raise ValueError("fill_na.mode must be mean|median|value")

    clip_iqr = ops.get("clip_outliers_iqr")
    if isinstance(clip_iqr, dict):
        k = float(clip_iqr.get("k", 1.5))
        cols = _normalize_columns_arg(clip_iqr.get("columns"), df2) if clip_iqr.get("columns") is not None else cols_default
        df2 = _coerce_to_numeric(df2, cols)
        for c in cols:
            if c not in df2.columns:
                continue
            s = df2[c]
            non_na = s.dropna()
            if len(non_na) < 4:
                continue
            q1 = non_na.quantile(0.25)
            q3 = non_na.quantile(0.75)
            iqr = q3 - q1
            if pd.isna(iqr) or iqr == 0:
                continue
            low = q1 - k * iqr
            high = q3 + k * iqr
            df2[c] = s.clip(lower=low, upper=high)

    rm_z = ops.get("remove_outliers_zscore")
    if isinstance(rm_z, dict):
        z = float(rm_z.get("z", 3.0))
        cols = _normalize_columns_arg(rm_z.get("columns"), df2) if rm_z.get("columns") is not None else cols_default
        df2 = _coerce_to_numeric(df2, cols)
        mask_keep = pd.Series(True, index=df2.index)
        for c in cols:
            if c not in df2.columns:
                continue
            s = df2[c]
            mu = s.mean(skipna=True)
            sigma = s.std(skipna=True, ddof=1)
            if pd.isna(mu) or pd.isna(sigma) or sigma == 0:
                continue
            zscores = (s - mu) / sigma
            bad = zscores.abs() > z
            bad = bad.fillna(False)
            mask_keep = mask_keep & (~bad)
        df2 = df2.loc[mask_keep].copy()

    if ops.get("normalize_minmax"):
        cols = cols_default
        df2 = _coerce_to_numeric(df2, cols)
        for c in cols:
            if c not in df2.columns:
                continue
            s = df2[c]
            mn = s.min(skipna=True)
            mx = s.max(skipna=True)
            if pd.isna(mn) or pd.isna(mx) or mx == mn:
                continue
            df2[c] = (s - mn) / (mx - mn)

    if ops.get("standardize_zscore"):
        cols = cols_default
        df2 = _coerce_to_numeric(df2, cols)
        for c in cols:
            if c not in df2.columns:
                continue
            s = df2[c]
            mu = s.mean(skipna=True)
            sigma = s.std(skipna=True, ddof=1)
            if pd.isna(mu) or pd.isna(sigma) or sigma == 0:
                continue
            df2[c] = (s - mu) / sigma

    if ops.get("trim_whitespace") or ops.get("lowercase"):
        cols = cols_default
        for c in cols:
            if c not in df2.columns:
                continue
            inferred = _infer_semantic_type(df2[c])
            if inferred != "text":
                continue
            s = df2[c].astype("string")
            if ops.get("trim_whitespace"):
                s = s.str.strip()
            if ops.get("lowercase"):
                s = s.str.lower()
            df2[c] = s

    return df2


def _get_numeric_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in df.columns:
        if _infer_semantic_type(df[c]) == "numeric":
            cols.append(str(c))
    return cols


def _quantiles(series: pd.Series, qs: List[float]) -> Dict[str, Any]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {}
    qv = s.quantile(qs).to_dict()
    out: Dict[str, Any] = {}
    for k, v in qv.items():
        out[str(k)] = _safe_float(v)
    return out


def _histogram_for_numeric(series: pd.Series, bins: int = 30, sample: int = 50000) -> Dict[str, Any]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    n = int(len(s))
    if n == 0:
        return {"edges": [], "counts": [], "n": 0, "sampled_n": 0, "stats": {}, "quantiles": {}}

    if n > sample:
        s = s.sample(n=sample, random_state=42)

    arr = s.to_numpy(dtype=float)
    counts, edges = np.histogram(arr, bins=bins)

    return {
        "n": int(n),
        "sampled_n": int(len(arr)),
        "edges": [float(x) for x in edges.tolist()],
        "counts": [int(x) for x in counts.tolist()],
        "stats": {
            "min": _safe_float(np.min(arr)),
            "max": _safe_float(np.max(arr)),
            "mean": _safe_float(np.mean(arr)),
            "std": _safe_float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
        },
        "quantiles": _quantiles(series, [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0]),
    }


def _corr_matrix(df: pd.DataFrame, cols: List[str]) -> Optional[Dict[str, Any]]:
    if len(cols) < 2:
        return None

    tmp = df[cols].copy()
    for c in cols:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")

    corr = tmp.corr(method="pearson")
    matrix: List[List[Optional[float]]] = []
    for i in range(len(cols)):
        row: List[Optional[float]] = []
        for j in range(len(cols)):
            row.append(_safe_float(corr.iloc[i, j]))
        matrix.append(row)

    return {"cols": cols, "matrix": matrix}


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/api/upload")
def api_upload():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file field."}), 400

    f = request.files["file"]
    filename = f.filename or "upload"
    raw = f.read()

    try:
        df, meta = _parse_by_extension(filename, raw)
        df.columns = [str(c) for c in df.columns]
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    dataset_id = str(uuid.uuid4())
    _DATASETS[dataset_id] = DatasetEntry(df=df, filename=filename)
    _evict_if_needed()

    return jsonify(
        {
            "ok": True,
            "dataset_id": dataset_id,
            "filename": filename,
            "meta": meta,
            "profile": _column_profile(df),
            "preview": _preview(df),
        }
    )


@app.post("/api/clean")
def api_clean():
    payload = request.get_json(silent=True) or {}
    dataset_id = payload.get("dataset_id")
    spec = payload.get("spec") or {}

    if not dataset_id:
        return jsonify({"ok": False, "error": "dataset_id required"}), 400

    try:
        entry = _get_dataset(str(dataset_id))
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    try:
        df2 = _apply_cleaning(entry.df, spec)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    _DATASETS[str(dataset_id)] = DatasetEntry(df=df2, filename=entry.filename)

    return jsonify(
        {
            "ok": True,
            "dataset_id": str(dataset_id),
            "profile": _column_profile(df2),
            "preview": _preview(df2),
        }
    )


@app.get("/api/export/csv")
def api_export_csv():
    dataset_id = request.args.get("dataset_id", "")
    try:
        entry = _get_dataset(dataset_id)
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    buf = io.StringIO()
    entry.df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")

    out_name = entry.filename.rsplit(".", 1)[0] + "_cleaned.csv"
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=out_name,
    )


@app.get("/api/export/jsonl")
def api_export_jsonl():
    dataset_id = request.args.get("dataset_id", "")
    try:
        entry = _get_dataset(dataset_id)
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    records = entry.df.replace({np.nan: None}).to_dict(orient="records")
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    data = ("\n".join(lines) + "\n").encode("utf-8")

    out_name = entry.filename.rsplit(".", 1)[0] + "_cleaned.jsonl"
    return send_file(
        io.BytesIO(data),
        mimetype="application/json; charset=utf-8",
        as_attachment=True,
        download_name=out_name,
    )


@app.post("/api/delete")
def api_delete():
    payload = request.get_json(silent=True) or {}
    dataset_id = payload.get("dataset_id")
    if not dataset_id:
        return jsonify({"ok": False, "error": "dataset_id required"}), 400
    _DATASETS.pop(str(dataset_id), None)
    return jsonify({"ok": True})


@app.get("/api/analytics")
def api_analytics():
    dataset_id = request.args.get("dataset_id", "")
    try:
        entry = _get_dataset(dataset_id)
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    df = entry.df
    total_rows = int(len(df)) if len(df) > 0 else 0

    missing = []
    for c in df.columns:
        miss = int(df[c].isna().sum())
        pct = (miss / total_rows * 100.0) if total_rows > 0 else 0.0
        missing.append({"col": str(c), "missing_count": miss, "missing_pct": float(pct)})

    numeric_cols = _get_numeric_columns(df)
    corr = _corr_matrix(df, numeric_cols)

    return jsonify(
        {
            "ok": True,
            "dataset_id": dataset_id,
            "total_rows": total_rows,
            "missing": missing,
            "numeric_columns": numeric_cols,
            "correlation": corr,
        }
    )


@app.get("/api/histogram")
def api_histogram():
    dataset_id = request.args.get("dataset_id", "")
    col = request.args.get("column", "")
    bins = int(request.args.get("bins", "30") or "30")
    sample = int(request.args.get("sample", "50000") or "50000")

    if not dataset_id or not col:
        return jsonify({"ok": False, "error": "dataset_id and column required"}), 400

    try:
        entry = _get_dataset(dataset_id)
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    df = entry.df
    if col not in df.columns:
        return jsonify({"ok": False, "error": "column_not_found"}), 404

    inferred = _infer_semantic_type(df[col])
    if inferred != "numeric":
        return jsonify({"ok": False, "error": "column_not_numeric"}), 400

    bins = max(5, min(200, bins))
    sample = max(1000, min(500000, sample))

    hist = _histogram_for_numeric(df[col], bins=bins, sample=sample)
    return jsonify({"ok": True, "column": col, "inferred_type": inferred, "hist": hist})


@app.get("/api/column_distribution")
def api_column_distribution():
    dataset_id = request.args.get("dataset_id", "")
    col = request.args.get("column", "")
    topn = int(request.args.get("topn", "30") or "30")

    if not dataset_id or not col:
        return jsonify({"ok": False, "error": "dataset_id and column required"}), 400

    try:
        entry = _get_dataset(dataset_id)
    except KeyError:
        return jsonify({"ok": False, "error": "dataset_not_found"}), 404

    df = entry.df
    if col not in df.columns:
        return jsonify({"ok": False, "error": "column_not_found"}), 404

    inferred = _infer_semantic_type(df[col])
    s = df[col]
    missing = int(s.isna().sum())
    unique = int(s.nunique(dropna=True))
    topn = max(5, min(100, topn))

    if inferred == "numeric":
        return jsonify(
            {
                "ok": True,
                "column": col,
                "inferred_type": inferred,
                "missing": missing,
                "unique": unique,
                "numeric": _histogram_for_numeric(s, bins=30, sample=50000),
            }
        )

    ss = s.astype("string")
    vc = ss.dropna().value_counts().head(topn)
    labels = [str(x) for x in vc.index.tolist()]
    counts = [int(x) for x in vc.values.tolist()]

    return jsonify(
        {
            "ok": True,
            "column": col,
            "inferred_type": inferred,
            "missing": missing,
            "unique": unique,
            "top_values": {"labels": labels, "counts": counts},
        }
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))  # Render/Railway 会设置 PORT
    app.run(host="0.0.0.0", port=port, debug=False)