import os
import math
import html
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
BASE_PATH = "results"
INPUT_CSV = os.path.join(
    BASE_PATH,
    "analysis_results",
    "enriched_high_priority_topics_from_raw_polar.csv"
)
OUTPUT_HTML = os.path.join(
    BASE_PATH,
    "analysis_results",
    "enriched_high_priority_topics_report.html"
)


# =========================================================
# HELPERS
# =========================================================
def safe_str(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def safe_float(value, default=None):
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value, default=None):
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return int(float(value))
    except Exception:
        return default


def fmt_number(value, digits=3):
    x = safe_float(value, None)
    if x is None:
        return "—"
    return f"{x:.{digits}f}"


def fmt_int(value):
    x = safe_int(value, None)
    if x is None:
        return "—"
    return f"{x:,}"


def esc(value):
    return html.escape(safe_str(value))


def split_pipe(text):
    raw = safe_str(text)
    if not raw.strip():
        return []
    return [x.strip() for x in raw.split("|") if x.strip()]


def render_list_block(text, empty="—"):
    items = split_pipe(text)
    if not items:
        return f"<div class='muted'>{esc(empty)}</div>"
    lis = "".join(f"<li>{esc(item)}</li>" for item in items)
    return f"<ul>{lis}</ul>"


def cluster_sort_key(name):
    order = {
        "Cluster A - Gender / eligibility / identity": 1,
        "Cluster B - Opening ceremony / Christianity": 2,
        "Cluster C - Secondary controversial themes": 3,
        "Cluster D - Unassigned / review": 4,
    }
    return order.get(name, 99)


# =========================================================
# LOAD
# =========================================================
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV)

required_cols = {
    "topic_id",
    "topic_cluster",
    "topic_subcluster",
    "cluster_role",
    "candidate_narrative",
    "global_score",
    "topic_observations",
}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Λείπουν στήλες από το input CSV: {sorted(missing)}")

df = df.copy()

# normalise sort helpers
df["_cluster_order"] = df["topic_cluster"].astype(str).map(cluster_sort_key)
df["_global_score_num"] = pd.to_numeric(df.get("global_score"), errors="coerce")
df["_topic_obs_num"] = pd.to_numeric(df.get("topic_observations"), errors="coerce")

df = df.sort_values(
    by=["_cluster_order", "_global_score_num", "_topic_obs_num"],
    ascending=[True, False, False]
).reset_index(drop=True)


# =========================================================
# SUMMARY
# =========================================================
total_topics = len(df)
cluster_counts = df["topic_cluster"].value_counts(dropna=False)

cluster_summary_rows = []
for cluster_name, group in sorted(df.groupby("topic_cluster", dropna=False), key=lambda x: cluster_sort_key(x[0])):
    cluster_summary_rows.append({
        "name": cluster_name,
        "count": len(group),
        "sum_score": group["_global_score_num"].fillna(0).sum(),
        "sum_obs": group["_topic_obs_num"].fillna(0).sum(),
    })


# =========================================================
# BUILD HTML
# =========================================================
html_parts = []

html_parts.append("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Enriched High-Priority Topics Report</title>
<style>
    body {
        font-family: Arial, Helvetica, sans-serif;
        margin: 32px;
        color: #222;
        line-height: 1.45;
        background: #fafafa;
    }
    h1, h2, h3 {
        color: #111;
    }
    h1 {
        margin-bottom: 8px;
    }
    .subtitle {
        color: #555;
        margin-bottom: 24px;
    }
    .summary-box {
        background: #fff;
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 16px 18px;
        margin-bottom: 24px;
    }
    .cluster-box {
        background: #fff;
        border: 1px solid #ddd;
        border-radius: 12px;
        padding: 18px;
        margin-bottom: 28px;
    }
    .topic-card {
        border: 1px solid #e3e3e3;
        border-radius: 10px;
        padding: 14px 16px;
        margin-top: 14px;
        background: #fcfcfc;
    }
    .meta-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(140px, 1fr));
        gap: 10px;
        margin: 12px 0 16px 0;
    }
    .meta-item {
        background: #fff;
        border: 1px solid #ececec;
        border-radius: 8px;
        padding: 8px 10px;
    }
    .meta-label {
        font-size: 12px;
        color: #666;
        margin-bottom: 4px;
    }
    .meta-value {
        font-size: 14px;
        font-weight: 600;
        color: #111;
    }
    .section {
        margin-top: 12px;
    }
    .section-title {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 6px;
    }
    .muted {
        color: #777;
    }
    ul {
        margin-top: 6px;
        margin-bottom: 6px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        margin-top: 8px;
        background: #fff;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px 10px;
        text-align: left;
        vertical-align: top;
    }
    th {
        background: #f1f1f1;
    }
    .small {
        font-size: 12px;
        color: #666;
    }
    .tag {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 999px;
        background: #eef2ff;
        color: #253b80;
        font-size: 12px;
        margin-right: 6px;
        margin-bottom: 6px;
    }
</style>
</head>
<body>
""")

html_parts.append("<h1>Enriched High-Priority Topics Report</h1>")
html_parts.append("<div class='subtitle'>Generated from enriched_high_priority_topics_from_raw_polar.csv</div>")

# Top summary
html_parts.append("<div class='summary-box'>")
html_parts.append(f"<h2>Summary</h2>")
html_parts.append(f"<p><strong>Total selected topics:</strong> {total_topics}</p>")

html_parts.append("<table>")
html_parts.append("<tr><th>Cluster</th><th>Topic Count</th><th>Sum Global Score</th><th>Sum Topic Observations</th></tr>")
for row in cluster_summary_rows:
    html_parts.append(
        f"<tr>"
        f"<td>{esc(row['name'])}</td>"
        f"<td>{fmt_int(row['count'])}</td>"
        f"<td>{fmt_number(row['sum_score'], 3)}</td>"
        f"<td>{fmt_int(row['sum_obs'])}</td>"
        f"</tr>"
    )
html_parts.append("</table>")
html_parts.append("</div>")

# Per cluster
for cluster_name, group in sorted(df.groupby("topic_cluster", dropna=False), key=lambda x: cluster_sort_key(x[0])):
    group = group.sort_values(by=["_global_score_num", "_topic_obs_num"], ascending=[False, False]).reset_index(drop=True)

    html_parts.append("<div class='cluster-box'>")
    html_parts.append(f"<h2>{esc(cluster_name)}</h2>")
    html_parts.append(
        f"<div class='small'>"
        f"Topics: {fmt_int(len(group))} | "
        f"Sum score: {fmt_number(group['_global_score_num'].fillna(0).sum(), 3)} | "
        f"Sum observations: {fmt_int(group['_topic_obs_num'].fillna(0).sum())}"
        f"</div>"
    )

    narratives = group["candidate_narrative"].dropna().astype(str).tolist()
    if narratives:
        html_parts.append("<div class='section'><div class='section-title'>Candidate narratives</div>")
        for item in sorted(set(narratives)):
            html_parts.append(f"<span class='tag'>{esc(item)}</span>")
        html_parts.append("</div>")

    for _, row in group.iterrows():
        topic_id = row["topic_id"]

        html_parts.append("<div class='topic-card'>")
        html_parts.append(f"<h3>{esc(topic_id)}</h3>")

        html_parts.append("<div class='meta-grid'>")
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Narrative</div><div class='meta-value'>{esc(row.get('candidate_narrative', '')) or '—'}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Cluster role</div><div class='meta-value'>{esc(row.get('cluster_role', '')) or '—'}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Global score</div><div class='meta-value'>{fmt_number(row.get('global_score'), 3)}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Topic observations</div><div class='meta-value'>{fmt_int(row.get('topic_observations'))}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Related dipoles</div><div class='meta-value'>{fmt_int(row.get('related_dipoles_count'))}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Related fellowships</div><div class='meta-value'>{fmt_int(row.get('related_fellowships_count'))}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Avg PI</div><div class='meta-value'>{fmt_number(row.get('avg_pi_from_analyzer'), 3)}</div></div>"
        )
        html_parts.append(
            f"<div class='meta-item'><div class='meta-label'>Max PI</div><div class='meta-value'>{fmt_number(row.get('max_pi_from_analyzer'), 3)}</div></div>"
        )
        html_parts.append("</div>")

        # phrases
        html_parts.append("<div class='section'>")
        html_parts.append("<div class='section-title'>Top phrases</div>")
        phrases_text = row.get("top_phrases_from_topics_json", "") or row.get("top_phrases_from_cluster_file", "")
        html_parts.append(render_list_block(phrases_text))
        html_parts.append("</div>")

        # entities
        html_parts.append("<div class='section'>")
        html_parts.append("<div class='section-title'>Top entities</div>")
        html_parts.append(render_list_block(row.get("top_entities", "")))
        html_parts.append("</div>")

        # fellowships
        html_parts.append("<div class='section'>")
        html_parts.append("<div class='section-title'>Related fellowships</div>")
        html_parts.append(render_list_block(row.get("related_fellowships", "")))
        html_parts.append("</div>")

        # dipoles
        html_parts.append("<div class='section'>")
        html_parts.append("<div class='section-title'>Related dipoles</div>")
        html_parts.append(render_list_block(row.get("related_dipoles", "")))
        html_parts.append("</div>")

        # analyzer labels
        html_parts.append("<div class='section'>")
        html_parts.append("<div class='section-title'>Analyzer labels</div>")
        html_parts.append(render_list_block(row.get("top_labels_from_analyzer", "")))
        html_parts.append("</div>")

        # fellowship member summary
        if safe_str(row.get("fellowship_members_summary", "")).strip():
            html_parts.append("<div class='section'>")
            html_parts.append("<div class='section-title'>Fellowship members summary</div>")
            html_parts.append(render_list_block(row.get("fellowship_members_summary", "")))
            html_parts.append("</div>")

        # optional manual notes fields
        notes_parts = []
        for field_name, label in [
            ("main_actor_entities", "Main actor entities"),
            ("main_fellowship_interpretation", "Main fellowship interpretation"),
            ("final_interpretive_notes", "Final interpretive notes"),
        ]:
            value = safe_str(row.get(field_name, "")).strip()
            if value:
                notes_parts.append(f"<p><strong>{esc(label)}:</strong> {esc(value)}</p>")

        if notes_parts:
            html_parts.append("<div class='section'>")
            html_parts.append("<div class='section-title'>Manual notes</div>")
            html_parts.extend(notes_parts)
            html_parts.append("</div>")

        html_parts.append("</div>")  # topic-card

    html_parts.append("</div>")  # cluster-box

html_parts.append("</body></html>")


# =========================================================
# SAVE
# =========================================================
os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write("".join(html_parts))

print("Ολοκληρώθηκε η δημιουργία του HTML report.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_HTML}")