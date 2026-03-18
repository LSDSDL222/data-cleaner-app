let currentDatasetId = null;
let currentFilename = null;
let uploadInFlight = false;

const $ = (id) => document.getElementById(id);

function setText(id, text) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || "";
}

function setDisabled(id, disabled) {
  const el = $(id);
  if (!el) return;
  el.disabled = !!disabled;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function getSelectedColumns() {
  const nodes = document.querySelectorAll('input[data-col="1"]');
  const cols = [];
  nodes.forEach((n) => {
    if (n.checked) cols.push(n.value);
  });
  return cols;
}

function renderColPicker(columns) {
  const wrap = $("colPicker");
  wrap.innerHTML = "";
  columns.forEach((c) => {
    const item = document.createElement("label");
    item.className = "colItem";
    item.innerHTML = `
      <input data-col="1" type="checkbox" value="${escapeHtml(c)}" />
      <span>${escapeHtml(c)}</span>
    `;
    wrap.appendChild(item);
  });
}

function fmtNum(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "";
  if (typeof x === "number") {
    return Number.isInteger(x)
      ? String(x)
      : x.toFixed(6).replace(/0+$/, "").replace(/\.$/, "");
  }
  return String(x);
}

function renderProfile(profile) {
  const wrap = $("profileTable");
  if (!profile || profile.length === 0) {
    wrap.innerHTML = `<div class="muted">暂无列信息</div>`;
    return;
  }

  const rows = profile
    .map((p) => {
      const ns = p.numeric_stats || null;
      const ts = p.text_stats || null;

      const numericHtml = ns
        ? `min=${fmtNum(ns.min)} max=${fmtNum(ns.max)} mean=${fmtNum(ns.mean)} std=${fmtNum(
            ns.std
          )} q25=${fmtNum(ns.q25)} median=${fmtNum(ns.median)} q75=${fmtNum(ns.q75)}`
        : "";

      const textHtml = ts
        ? `avg_len=${fmtNum(ts.avg_len)} max_len=${fmtNum(ts.max_len)}`
        : "";

      return `
        <tr>
          <td>${escapeHtml(p.name)}</td>
          <td>${escapeHtml(p.inferred_type)}</td>
          <td>${escapeHtml(String(p.missing))}</td>
          <td>${escapeHtml(String(p.unique))}</td>
          <td class="small">${escapeHtml(numericHtml)}</td>
          <td class="small">${escapeHtml(textHtml)}</td>
        </tr>
      `;
    })
    .join("");

  wrap.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>列名</th>
          <th>推断类型</th>
          <th>缺失数量</th>
          <th>唯一值数量</th>
          <th>数值列统计</th>
          <th>文本列统计</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderPreview(preview) {
  const wrap = $("previewBox");
  if (!preview) {
    wrap.innerHTML = `<div class="muted">暂无预览</div>`;
    window.__lastPreviewCols = [];
    return;
  }

  const cols = preview.columns || [];
  const rows = preview.rows || [];
  window.__lastPreviewCols = cols;

  const thead = cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const tbody = rows
    .map((r) => {
      const tds = cols
        .map((c) => {
          const v = r[c];
          return `<td>${v === null || v === undefined ? "" : escapeHtml(String(v))}</td>`;
        })
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  wrap.innerHTML = `
    <div class="muted">总行数：${escapeHtml(String(preview.total_rows))}，总列数：${escapeHtml(
    String(preview.total_cols)
  )}（预览前 ${escapeHtml(String(preview.preview_rows))} 行）</div>
    <table>
      <thead><tr>${thead}</tr></thead>
      <tbody>${tbody}</tbody>
    </table>
  `;
}

function renderMeta(meta) {
  const box = $("metaBox");
  if (!meta) {
    box.innerHTML = "";
    return;
  }
  box.innerHTML = `
    <div><span class="badge">format</span> ${escapeHtml(meta.format || "")}</div>
    ${meta.encoding ? `<div><span class="badge">encoding</span> ${escapeHtml(meta.encoding)}</div>` : ""}
    ${meta.json_shape ? `<div><span class="badge">json_shape</span> ${escapeHtml(meta.json_shape)}</div>` : ""}
    ${meta.lines !== undefined ? `<div><span class="badge">lines</span> ${escapeHtml(String(meta.lines))}</div>` : ""}
    ${meta.sheet ? `<div><span class="badge">sheet</span> ${escapeHtml(meta.sheet)}</div>` : ""}
  `;
}

async function refreshAnalytics() {
  if (!currentDatasetId) return;
  if (!window.Plotly) return;

  const res = await fetch(`/api/analytics?dataset_id=${encodeURIComponent(currentDatasetId)}`);
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "analytics_failed");

  renderMissingChart(data.missing || []);
  renderCorrChart(data.correlation);

  populateHistogramSelect(data.numeric_columns || []);
  populateColumnSelectFromPreview();

  const histCol = $("histSelect").value;
  if (histCol) {
    await renderHistogramForColumn(histCol);
  }

  const col = $("colSelect").value;
  if (col) {
    await renderColumnExplorer(col);
  }
}

function renderMissingChart(missing) {
  if (!window.Plotly) return;

  const cols = missing.map((x) => x.col);
  const pct = missing.map((x) => x.missing_pct);

  const trace = {
    type: "bar",
    x: cols,
    y: pct,
    marker: { color: "#4c8dff" },
    hovertemplate: "%{x}<br>missing=%{y:.2f}%<extra></extra>",
  };

  const layout = {
    margin: { l: 40, r: 10, t: 10, b: 80 },
    yaxis: { title: "Missing %", rangemode: "tozero" },
    xaxis: { tickangle: -35 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e8efff" },
  };

  Plotly.newPlot("chartMissing", [trace], layout, { displayModeBar: false, responsive: true });
}

async function renderHistogramForColumn(col) {
  if (!window.Plotly) return;

  setText("histInfo", "加载直方图中...");
  const res = await fetch(
    `/api/histogram?dataset_id=${encodeURIComponent(currentDatasetId)}&column=${encodeURIComponent(col)}&bins=30&sample=50000`
  );
  const data = await res.json();
  if (!data.ok) {
    setText("histInfo", `失败：${data.error || "hist_failed"}`);
    Plotly.purge("chartHist");
    return;
  }

  const edges = data.hist.edges || [];
  const counts = data.hist.counts || [];
  const mids = [];
  for (let i = 0; i < edges.length - 1; i++) {
    mids.push((edges[i] + edges[i + 1]) / 2);
  }

  const trace = {
    type: "bar",
    x: mids,
    y: counts,
    marker: { color: "#2f6fe6" },
    hovertemplate: "x=%{x:.4f}<br>count=%{y}<extra></extra>",
  };

  const layout = {
    margin: { l: 40, r: 10, t: 10, b: 40 },
    xaxis: { title: col },
    yaxis: { title: "Count", rangemode: "tozero" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e8efff" },
  };

  Plotly.newPlot("chartHist", [trace], layout, { displayModeBar: false, responsive: true });

  const st = data.hist.stats || {};
  const qs = data.hist.quantiles || {};
  setText(
    "histInfo",
    `n=${data.hist.n}, sampled=${data.hist.sampled_n}, min=${fmtNum(st.min)}, max=${fmtNum(st.max)}, mean=${fmtNum(
      st.mean
    )}, std=${fmtNum(st.std)}, p50=${fmtNum(qs["0.5"])}`
  );
}

function renderCorrChart(corr) {
  if (!window.Plotly) return;

  if (!corr || !corr.cols || corr.cols.length < 2) {
    setText("corrHint", "数值列少于 2 列，相关系数热力图不显示。");
    Plotly.purge("chartCorr");
    return;
  }
  setText("corrHint", "");

  const trace = {
    type: "heatmap",
    x: corr.cols,
    y: corr.cols,
    z: corr.matrix,
    colorscale: "RdBu",
    zmin: -1,
    zmax: 1,
    hovertemplate: "%{x} vs %{y}<br>%{z:.4f}<extra></extra>",
  };

  const layout = {
    margin: { l: 80, r: 10, t: 10, b: 80 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e8efff" },
  };

  Plotly.newPlot("chartCorr", [trace], layout, { displayModeBar: false, responsive: true });
}

function populateHistogramSelect(numericCols) {
  const sel = $("histSelect");
  sel.innerHTML = "";

  if (!numericCols || numericCols.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "（无数值列）";
    sel.appendChild(opt);
    return;
  }

  numericCols.forEach((c, idx) => {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
    if (idx === 0) sel.value = c;
  });
}

function populateColumnSelectFromPreview() {
  const sel = $("colSelect");
  sel.innerHTML = "";
  const cols = window.__lastPreviewCols || [];
  cols.forEach((c, idx) => {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
    if (idx === 0) sel.value = c;
  });
}

async function renderColumnExplorer(col) {
  if (!window.Plotly) return;

  setText("colStats", "加载中...");
  Plotly.purge("chartCol");

  const res = await fetch(
    `/api/column_distribution?dataset_id=${encodeURIComponent(currentDatasetId)}&column=${encodeURIComponent(col)}&topn=30`
  );
  const data = await res.json();
  if (!data.ok) {
    setText("colStats", `失败：${data.error || "column_failed"}`);
    return;
  }

  const base = `type=${data.inferred_type}, missing=${data.missing}, unique=${data.unique}`;

  if (data.inferred_type === "numeric" && data.numeric) {
    const edges = data.numeric.edges || [];
    const counts = data.numeric.counts || [];
    const mids = [];
    for (let i = 0; i < edges.length - 1; i++) mids.push((edges[i] + edges[i + 1]) / 2);

    const qs = data.numeric.quantiles || {};
    const qText = `p0=${fmtNum(qs["0.0"])}, p1=${fmtNum(qs["0.01"])}, p5=${fmtNum(qs["0.05"])}, p25=${fmtNum(
      qs["0.25"]
    )}, p50=${fmtNum(qs["0.5"])}, p75=${fmtNum(qs["0.75"])}, p95=${fmtNum(qs["0.95"])}, p99=${fmtNum(
      qs["0.99"]
    )}, p100=${fmtNum(qs["1.0"])}`;

    setText("colStats", `${base}\n${qText}`);

    Plotly.newPlot(
      "chartCol",
      [
        {
          type: "bar",
          x: mids,
          y: counts,
          marker: { color: "#4c8dff" },
          hovertemplate: "x=%{x:.4f}<br>count=%{y}<extra></extra>",
        },
      ],
      {
        margin: { l: 40, r: 10, t: 10, b: 40 },
        xaxis: { title: col },
        yaxis: { title: "Count", rangemode: "tozero" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: "#e8efff" },
      },
      { displayModeBar: false, responsive: true }
    );

    return;
  }

  const tv = data.top_values || { labels: [], counts: [] };
  setText("colStats", `${base}\nTop ${tv.labels.length} values`);

  Plotly.newPlot(
    "chartCol",
    [
      {
        type: "bar",
        x: tv.labels,
        y: tv.counts,
        marker: { color: "#2f6fe6" },
        hovertemplate: "%{x}<br>%{y}<extra></extra>",
      },
    ],
    {
      margin: { l: 40, r: 10, t: 10, b: 120 },
      xaxis: { tickangle: -35 },
      yaxis: { title: "Count", rangemode: "tozero" },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8efff" },
    },
    { displayModeBar: false, responsive: true }
  );
}

async function uploadFile(file) {
  if (!file) return;
  if (uploadInFlight) return;

  uploadInFlight = true;
  setText("uploadInfo", "上传中...");

  try {
    const fd = new FormData();
    fd.append("file", file);

    const res = await fetch("/api/upload", { method: "POST", body: fd });

    const contentType = (res.headers.get("content-type") || "").toLowerCase();
    let data = null;

    if (contentType.includes("application/json")) {
      data = await res.json();
    } else {
      const text = await res.text();
      throw new Error(`服务器返回非JSON（HTTP ${res.status}）：${text.slice(0, 200)}`);
    }

    if (!res.ok) {
      throw new Error((data && data.error) || `HTTP ${res.status}`);
    }
    if (!data || !data.ok) {
      throw new Error((data && data.error) || "unknown error");
    }

    currentDatasetId = data.dataset_id;
    currentFilename = data.filename;

    setText("uploadInfo", `已解析：${currentFilename}（dataset_id=${currentDatasetId}）`);
    renderMeta(data.meta);
    renderProfile(data.profile);
    renderPreview(data.preview);
    renderColPicker((data.preview && data.preview.columns) || []);

    setDisabled("btnApply", false);
    setDisabled("btnReset", false);
    setDisabled("btnExportCsv", false);
    setDisabled("btnExportJsonl", false);

    await refreshAnalytics();
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    setText("uploadInfo", `失败：${msg}`);
    renderMeta(null);
    renderProfile(null);
    renderPreview(null);
    const colPicker = $("colPicker");
    if (colPicker) colPicker.innerHTML = "";

    setDisabled("btnApply", true);
    setDisabled("btnReset", true);
    setDisabled("btnExportCsv", true);
    setDisabled("btnExportJsonl", true);

    if (window.Plotly) {
      Plotly.purge("chartMissing");
      Plotly.purge("chartHist");
      Plotly.purge("chartCorr");
      Plotly.purge("chartCol");
    }
    setText("histInfo", "");
    setText("corrHint", "");
    setText("colStats", "");
  } finally {
    uploadInFlight = false;
    const input = $("fileInput");
    if (input) input.value = "";
  }
}

function buildCleanSpec() {
  const cols = getSelectedColumns();
  const spec = {
    columns: cols.length > 0 ? cols : null,
    ops: {
      drop_duplicates: $("opDropDup").checked,
      drop_na_rows: $("opDropNaRows").checked,
      normalize_minmax: $("opMinMax").checked,
      standardize_zscore: $("opStdZ").checked,
      trim_whitespace: $("opTrim").checked,
      lowercase: $("opLower").checked,
    },
  };

  const fillMode = $("fillMode").value;
  if (fillMode) {
    const fill = { mode: fillMode };
    if (fillMode === "value") {
      const v = $("fillValue").value;
      fill.value = v === "" ? "" : v;
    }
    spec.ops.fill_na = fill;
  }

  if ($("opClipIqr").checked) {
    const k = Number($("iqrK").value || "1.5");
    spec.ops.clip_outliers_iqr = { k: Number.isFinite(k) ? k : 1.5 };
  }

  if ($("opRemoveZ").checked) {
    const z = Number($("zThresh").value || "3.0");
    spec.ops.remove_outliers_zscore = { z: Number.isFinite(z) ? z : 3.0 };
  }

  return spec;
}

async function applyCleaning() {
  if (!currentDatasetId) return;

  setText("cleanInfo", "清洗中...");
  setDisabled("btnApply", true);

  const payload = {
    dataset_id: currentDatasetId,
    spec: buildCleanSpec(),
  };

  const res = await fetch("/api/clean", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json();

  if (!data.ok) {
    setText("cleanInfo", `失败：${data.error || "unknown error"}`);
    setDisabled("btnApply", false);
    return;
  }

  renderProfile(data.profile);
  renderPreview(data.preview);

  await refreshAnalytics();

  setText("cleanInfo", "完成：已应用清洗（当前数据已更新）");
  setDisabled("btnApply", false);
}

async function resetDataset() {
  if (!currentDatasetId) return;

  await fetch("/api/delete", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ dataset_id: currentDatasetId }),
  });

  currentDatasetId = null;
  currentFilename = null;

  renderMeta(null);
  renderProfile(null);
  renderPreview(null);
  $("colPicker").innerHTML = "";
  setText("uploadInfo", "");
  setText("cleanInfo", "");
  setText("histInfo", "");
  setText("corrHint", "");
  setText("colStats", "");

  setDisabled("btnApply", true);
  setDisabled("btnReset", true);
  setDisabled("btnExportCsv", true);
  setDisabled("btnExportJsonl", true);

  if (window.Plotly) {
    Plotly.purge("chartMissing");
    Plotly.purge("chartHist");
    Plotly.purge("chartCorr");
    Plotly.purge("chartCol");
  }
}

function exportFile(kind) {
  if (!currentDatasetId) return;
  const url =
    kind === "csv"
      ? `/api/export/csv?dataset_id=${encodeURIComponent(currentDatasetId)}`
      : `/api/export/jsonl?dataset_id=${encodeURIComponent(currentDatasetId)}`;
  window.location.href = url;
}

function setupDropzone() {
  const dz = $("dropzone");
  const input = $("fileInput");

  dz.addEventListener("click", () => input.click());

  dz.addEventListener("dragover", (e) => {
    e.preventDefault();
    dz.classList.add("hover");
  });

  dz.addEventListener("dragleave", () => dz.classList.remove("hover"));

  dz.addEventListener("drop", async (e) => {
    e.preventDefault();
    dz.classList.remove("hover");

    const file = e.dataTransfer.files && e.dataTransfer.files[0];
    if (file) {
      setText("uploadInfo", `已选择：${file.name}（${file.size} bytes），准备上传...`);
      await uploadFile(file);
    }
  });

  input.addEventListener("change", async () => {
    const file = input.files && input.files[0];
    if (file) {
      setText("uploadInfo", `已选择：${file.name}（${file.size} bytes），准备上传...`);
      await uploadFile(file);
    }
  });
}

window.addEventListener("DOMContentLoaded", () => {
  setupDropzone();

  setDisabled("btnApply", true);
  setDisabled("btnReset", true);
  setDisabled("btnExportCsv", true);
  setDisabled("btnExportJsonl", true);

  $("btnApply").addEventListener("click", async () => {
    await applyCleaning();
  });

  $("btnReset").addEventListener("click", async () => {
    await resetDataset();
  });

  $("btnExportCsv").addEventListener("click", () => exportFile("csv"));
  $("btnExportJsonl").addEventListener("click", () => exportFile("jsonl"));

  $("histSelect").addEventListener("change", async (e) => {
    const col = e.target.value;
    if (col) await renderHistogramForColumn(col);
  });

  $("colSelect").addEventListener("change", async (e) => {
    const col = e.target.value;
    if (col) await renderColumnExplorer(col);
  });
});