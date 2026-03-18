# app.py (Streamlit version with advanced cleaning)
import streamlit as st
import pandas as pd
import numpy as np
import json
import io
from charset_normalizer import from_bytes

# ========== 与你原代码一致的工具函数 ==========
def _best_effort_decode(raw: bytes) -> tuple[str, str]:
    for enc in ["utf-8-sig", "utf-8", "gb18030", "gbk"]:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            pass
    best = from_bytes(raw).best()
    if best is not None:
        return str(best), best.encoding or "detected"
    return raw.decode("utf-8", errors="replace"), "utf-8(replace)"

def _parse_csv(raw: bytes) -> pd.DataFrame:
    text, _ = _best_effort_decode(raw)
    return pd.read_csv(io.StringIO(text), sep=None, engine="python")

def _parse_json(raw: bytes) -> pd.DataFrame:
    text, _ = _best_effort_decode(raw)
    obj = json.loads(text)
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    elif isinstance(obj, dict):
        try:
            df_try = pd.DataFrame(obj)
            if len(df_try.columns) > 0 and len(df_try) > 1:
                return df_try
        except Exception:
            pass
        return pd.json_normalize(obj)
    raise ValueError("Unsupported JSON root type")

def _parse_jsonl(raw: bytes) -> pd.DataFrame:
    text, _ = _best_effort_decode(raw)
    rows = []
    for i, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if s:
            rows.append(json.loads(s))
    return pd.json_normalize(rows)

def _parse_txt(raw: bytes) -> pd.DataFrame:
    text, _ = _best_effort_decode(raw)
    lines = [ln.rstrip("\n\r") for ln in text.splitlines()]
    return pd.DataFrame({"text": lines})

def _parse_xlsx(raw: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(raw), engine="openpyxl")

def _infer_semantic_type(series: pd.Series) -> str:
    s = series
    if pd.api.types.is_bool_dtype(s): return "bool"
    if pd.api.types.is_numeric_dtype(s): return "numeric"
    if pd.api.types.is_datetime64_any_dtype(s): return "datetime"
    non_na = s.dropna()
    if len(non_na) == 0: return "empty"
    num_ratio = pd.to_numeric(non_na, errors="coerce").notna().mean()
    if num_ratio >= 0.9: return "numeric"
    dt_ratio = pd.to_datetime(non_na, errors="coerce").notna().mean()
    if dt_ratio >= 0.9: return "datetime"
    return "text"

def _safe_float(x) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None

def _apply_cleaning(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    df2 = df.copy()
    cols_default = [str(c) for c in df2.columns]
    ops = spec.get("ops", {})

    # Drop duplicates
    if ops.get("drop_duplicates"):
        df2 = df2.drop_duplicates()

    # Drop NA rows
    if ops.get("drop_na_rows"):
        df2 = df2.dropna()

    # Fill NA
    fill_na = ops.get("fill_na")
    if fill_na:
        mode = fill_na.get("mode")
        value = fill_na.get("value")
        cols = fill_na.get("columns", cols_default)
        if mode in ("mean", "median"):
            for c in cols:
                if c in df2.columns:
                    s = pd.to_numeric(df2[c], errors="coerce")
                    v = s.mean() if mode == "mean" else s.median()
                    if pd.notna(v):
                        df2[c] = df2[c].fillna(v)
        elif mode == "value":
            for c in cols:
                if c in df2.columns:
                    df2[c] = df2[c].fillna(value)

    # IQR outlier clipping
    clip_iqr = ops.get("clip_outliers_iqr")
    if clip_iqr:
        k = float(clip_iqr.get("k", 1.5))
        cols = clip_iqr.get("columns", cols_default)
        for c in cols:
            if c in df2.columns:
                s = pd.to_numeric(df2[c], errors="coerce")
                q1, q3 = s.quantile([0.25, 0.75])
                iqr = q3 - q1
                if pd.notna(iqr) and iqr != 0:
                    low, high = q1 - k * iqr, q3 + k * iqr
                    df2[c] = s.clip(lower=low, upper=high)

    # Z-score outlier removal
    rm_z = ops.get("remove_outliers_zscore")
    if rm_z:
        z = float(rm_z.get("z", 3.0))
        cols = rm_z.get("columns", cols_default)
        mask = pd.Series(True, index=df2.index)
        for c in cols:
            if c in df2.columns:
                s = pd.to_numeric(df2[c], errors="coerce")
                mu, sigma = s.mean(), s.std(ddof=1)
                if pd.notna(mu) and pd.notna(sigma) and sigma != 0:
                    zscores = (s - mu) / sigma
                    mask &= ~(zscores.abs() > z)
        df2 = df2[mask].copy()

    # MinMax normalize
    if ops.get("normalize_minmax"):
        for c in cols_default:
            if c in df2.columns:
                s = pd.to_numeric(df2[c], errors="coerce")
                mn, mx = s.min(), s.max()
                if pd.notna(mn) and pd.notna(mx) and mx != mn:
                    df2[c] = (s - mn) / (mx - mn)

    # Z-score standardize
    if ops.get("standardize_zscore"):
        for c in cols_default:
            if c in df2.columns:
                s = pd.to_numeric(df2[c], errors="coerce")
                mu, sigma = s.mean(), s.std(ddof=1)
                if pd.notna(mu) and pd.notna(sigma) and sigma != 0:
                    df2[c] = (s - mu) / sigma

    # Text processing
    if ops.get("trim_whitespace") or ops.get("lowercase"):
        for c in cols_default:
            if c in df2.columns and _infer_semantic_type(df2[c]) == "text":
                s = df2[c].astype("string")
                if ops.get("trim_whitespace"):
                    s = s.str.strip()
                if ops.get("lowercase"):
                    s = s.str.lower()
                df2[c] = s

    return df2

# ========== Streamlit UI ==========
st.set_page_config(page_title="🧹 Advanced Data Cleaner", layout="wide")
st.title("🧹 Advanced Data Cleaning Tool")

uploaded_file = st.file_uploader("Upload CSV/JSON/JSONL/TXT/XLSX", type=["csv", "json", "jsonl", "txt", "xlsx"])

if uploaded_file is not None:
    filename = uploaded_file.name
    raw = uploaded_file.getvalue()

    # Parse file
    try:
        if filename.endswith(".csv"):
            df = _parse_csv(raw)
        elif filename.endswith(".json"):
            df = _parse_json(raw)
        elif filename.endswith(".jsonl"):
            df = _parse_jsonl(raw)
        elif filename.endswith(".txt"):
            df = _parse_txt(raw)
        elif filename.endswith(".xlsx"):
            df = _parse_xlsx(raw)
        else:
            st.error("Unsupported file type")
            st.stop()
    except Exception as e:
        st.error(f"Parse error: {e}")
        st.stop()

    df.columns = [str(c) for c in df.columns]
    st.subheader("📊 Original Data")
    st.dataframe(df.head(50))

    # Cleaning options
    st.subheader("🔧 Cleaning Options")
    col1, col2 = st.columns(2)

    with col1:
        drop_dup = st.checkbox("Drop duplicates")
        drop_na_rows = st.checkbox("Drop rows with any NA")
        trim_ws = st.checkbox("Trim whitespace")
        lowercase = st.checkbox("Lowercase text")

    with col2:
        fill_mode = st.selectbox("Fill NA mode", ["None", "Mean", "Median", "Custom Value"])
        fill_value = st.text_input("Custom fill value", value="") if fill_mode == "Custom Value" else None

    # Outlier handling
    st.subheader("📈 Outlier Handling")
    outlier_col1, outlier_col2 = st.columns(2)
    with outlier_col1:
        clip_iqr = st.checkbox("Clip outliers (IQR)")
        iqr_k = st.slider("IQR multiplier (k)", 1.0, 3.0, 1.5) if clip_iqr else 1.5
    with outlier_col2:
        rm_zscore = st.checkbox("Remove outliers (Z-score)")
        z_threshold = st.slider("Z-score threshold", 1.0, 5.0, 3.0) if rm_zscore else 3.0

    # Normalization
    st.subheader("📏 Normalization")
    norm_col1, norm_col2 = st.columns(2)
    with norm_col1:
        minmax_norm = st.checkbox("MinMax Normalize")
    with norm_col2:
        zscore_std = st.checkbox("Z-score Standardize")

    # Build spec
    spec = {
        "ops": {
            "drop_duplicates": drop_dup,
            "drop_na_rows": drop_na_rows,
            "trim_whitespace": trim_ws,
            "lowercase": lowercase,
            "normalize_minmax": minmax_norm,
            "standardize_zscore": zscore_std,
        }
    }

    if fill_mode != "None":
        spec["ops"]["fill_na"] = {
            "mode": fill_mode.lower(),
            "value": fill_value,
        }

    if clip_iqr:
        spec["ops"]["clip_outliers_iqr"] = {"k": iqr_k}

    if rm_zscore:
        spec["ops"]["remove_outliers_zscore"] = {"z": z_threshold}

    # Apply cleaning
    if st.button("✨ Apply Cleaning"):
        try:
            df_clean = _apply_cleaning(df, spec)
            st.session_state["cleaned_df"] = df_clean
            st.session_state["original_filename"] = filename
            st.success("Cleaning applied!")
        except Exception as e:
            st.error(f"Cleaning error: {e}")

    # Show result & download
    if "cleaned_df" in st.session_state:
        st.subheader("✅ Cleaned Data")
        st.dataframe(st.session_state["cleaned_df"].head(50))

        # Export options
        export_format = st.radio("Export format", ["CSV", "JSONL"], horizontal=True)
        if export_format == "CSV":
            csv = st.session_state["cleaned_df"].to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Cleaned CSV",
                csv,
                f"{st.session_state['original_filename'].rsplit('.',1)[0]}_cleaned.csv",
                "text/csv"
            )
        else:
            records = st.session_state["cleaned_df"].replace({np.nan: None}).to_dict(orient="records")
            jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode('utf-8')
            st.download_button(
                "📥 Download Cleaned JSONL",
                jsonl,
                f"{st.session_state['original_filename'].rsplit('.',1)[0]}_cleaned.jsonl",
                "application/json"
            )