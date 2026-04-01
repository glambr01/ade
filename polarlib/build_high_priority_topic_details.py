import os
import re
from collections import Counter

import pandas as pd


# =========================================================
# CONFIG
# =========================================================
RESULTS_DIR = "results"

REFINED_MASTER_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "refined_master_topic_analysis.csv"
)
ANALYZER_RESULTS_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "analyzer_results.csv"
)

# optional
MATCHED_RESULTS_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "matched_results.csv"
)

OUTPUT_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "high_priority_topic_details.csv"
)


# =========================================================
# HELPERS
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def top_items_with_counts(items, limit=10):
    cleaned = [normalize_text(x) for x in items if normalize_text(x)]
    counts = Counter(cleaned)
    return [f"{item} ({count})" for item, count in counts.most_common(limit)]


def safe_to_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def first_existing_column(columns, candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


# =========================================================
# LOAD
# =========================================================
if not os.path.exists(REFINED_MASTER_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {REFINED_MASTER_CSV}")

if not os.path.exists(ANALYZER_RESULTS_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {ANALYZER_RESULTS_CSV}")

refined_df = pd.read_csv(REFINED_MASTER_CSV)
analyzer_df = pd.read_csv(ANALYZER_RESULTS_CSV)

matched_df = pd.DataFrame()
if os.path.exists(MATCHED_RESULTS_CSV):
    matched_df = pd.read_csv(MATCHED_RESULTS_CSV)

required_refined_cols = {
    "topic_id",
    "global_score",
    "topic_observations",
    "num_dipoles",
    "top_phrases",
    "candidate_narrative",
    "relevance_label",
    "priority_for_review",
}
missing_refined = required_refined_cols - set(refined_df.columns)
if missing_refined:
    raise ValueError(
        f"Λείπουν στήλες από refined_master_topic_analysis.csv: {sorted(missing_refined)}"
    )

required_analyzer_cols = {"topic", "dipole", "pi", "obs", "label"}
missing_analyzer = required_analyzer_cols - set(analyzer_df.columns)
if missing_analyzer:
    raise ValueError(
        f"Λείπουν στήλες από analyzer_results.csv: {sorted(missing_analyzer)}"
    )


# =========================================================
# SELECT TOPICS TO KEEP
# =========================================================
# Κρατάμε:
# 1) strong_candidate
# 2) candidate
# 3) possible_candidate with priority_for_review = high

selected_topics_df = refined_df[
    (
        refined_df["relevance_label"].isin(["strong_candidate", "candidate"])
    )
    |
    (
        (refined_df["relevance_label"] == "possible_candidate")
        & (refined_df["priority_for_review"] == "high")
    )
].copy()

selected_topics_df["topic_id"] = selected_topics_df["topic_id"].astype(str)
selected_topic_ids = set(selected_topics_df["topic_id"].tolist())

print(f"Selected topics for detailed extraction: {len(selected_topic_ids)}")


# =========================================================
# PREP ANALYZER
# =========================================================
analyzer_df = analyzer_df.copy()
analyzer_df["topic"] = analyzer_df["topic"].astype(str)

selected_analyzer = analyzer_df[analyzer_df["topic"].isin(selected_topic_ids)].copy()

selected_analyzer["pi_num"] = safe_to_numeric(selected_analyzer["pi"])
selected_analyzer["obs_num"] = safe_to_numeric(selected_analyzer["obs"])


# =========================================================
# OPTIONAL MATCHED RESULTS SUPPORT
# =========================================================
matched_topic_col = None
matched_label_col = None
matched_text_col = None

if not matched_df.empty:
    matched_topic_col = first_existing_column(
        matched_df.columns, ["topic_id", "topic"]
    )
    matched_label_col = first_existing_column(
        matched_df.columns,
        ["matched_narrative_category", "narrative_category", "label"]
    )
    matched_text_col = first_existing_column(
        matched_df.columns,
        [
            "interpretation",
            "matched_trigger_keywords",
            "trigger_keywords",
            "topic_phrases",
            "top_phrases"
        ]
    )

    if matched_topic_col is not None:
        matched_df = matched_df.copy()
        matched_df["__topic_id__"] = matched_df[matched_topic_col].astype(str)
        matched_df = matched_df[matched_df["__topic_id__"].isin(selected_topic_ids)].copy()


# =========================================================
# BUILD DETAIL ROWS
# =========================================================
detail_rows = []

for _, topic_row in selected_topics_df.iterrows():
    topic_id = str(topic_row["topic_id"])

    topic_an = selected_analyzer[selected_analyzer["topic"] == topic_id].copy()

    dipoles = topic_an["dipole"].dropna().astype(str).tolist()
    labels = topic_an["label"].dropna().astype(str).tolist()

    pi_vals = topic_an["pi_num"].dropna()
    obs_vals = topic_an["obs_num"].dropna()

    # optional matched summaries
    matched_rows_count = 0
    matched_label_summary = ""
    matched_text_summary = ""

    if not matched_df.empty and matched_topic_col is not None:
        topic_matched = matched_df[matched_df["__topic_id__"] == topic_id].copy()
        matched_rows_count = len(topic_matched)

        if matched_label_col is not None:
            matched_label_summary = " | ".join(
                top_items_with_counts(
                    topic_matched[matched_label_col].dropna().astype(str).tolist(),
                    limit=5
                )
            )

        if matched_text_col is not None:
            matched_text_summary = " | ".join(
                top_items_with_counts(
                    topic_matched[matched_text_col].dropna().astype(str).tolist(),
                    limit=5
                )
            )

    detail_rows.append({
        # topic identity
        "topic_id": topic_id,

        # priority and filtering metadata
        "candidate_narrative": topic_row.get("candidate_narrative", ""),
        "relevance_label": topic_row.get("relevance_label", ""),
        "priority_for_review": topic_row.get("priority_for_review", ""),
        "narrative_hit_count": topic_row.get("narrative_hit_count", ""),

        # global topic metrics
        "global_score": topic_row.get("global_score", ""),
        "topic_observations": topic_row.get("topic_observations", ""),
        "num_dipoles_from_master": topic_row.get("num_dipoles", ""),
        "dt": topic_row.get("dt", ""),
        "mt": topic_row.get("mt", ""),
        "maxm": topic_row.get("maxm", ""),
        "minm": topic_row.get("minm", ""),
        "stdm": topic_row.get("stdm", ""),
        "avgm": topic_row.get("avgm", ""),

        # phrases
        "top_phrases": topic_row.get("top_phrases", ""),

        # analyzer summaries
        "analyzer_rows": len(topic_an),
        "unique_dipoles_in_analyzer": topic_an["dipole"].nunique(dropna=True),
        "top_dipoles": " | ".join(top_items_with_counts(dipoles, limit=10)),
        "top_labels": " | ".join(top_items_with_counts(labels, limit=10)),
        "avg_pi": float(pi_vals.mean()) if not pi_vals.empty else None,
        "max_pi": float(pi_vals.max()) if not pi_vals.empty else None,
        "min_pi": float(pi_vals.min()) if not pi_vals.empty else None,
        "sum_obs_in_analyzer": int(obs_vals.sum()) if not obs_vals.empty else 0,
        "max_obs_in_analyzer": int(obs_vals.max()) if not obs_vals.empty else 0,
        "avg_obs_in_analyzer": float(obs_vals.mean()) if not obs_vals.empty else None,

        # interpretive support from matched_results if available
        "matched_rows_count": matched_rows_count,
        "matched_label_summary": matched_label_summary,
        "matched_text_summary": matched_text_summary,

        # manual annotation placeholders
        "keep_for_final_analysis": "",
        "topic_cluster": "",
        "final_notes": ""
    })

details_df = pd.DataFrame(detail_rows)


# =========================================================
# SORT
# =========================================================
priority_rank = {"high": 3, "medium": 2, "low": 1}
relevance_rank = {
    "strong_candidate": 6,
    "candidate": 5,
    "possible_candidate": 4,
    "needs_manual_review": 3,
    "low_signal": 2,
    "generic/noisy": 1
}

details_df["_priority_rank"] = details_df["priority_for_review"].map(priority_rank).fillna(0)
details_df["_relevance_rank"] = details_df["relevance_label"].map(relevance_rank).fillna(0)

details_df = details_df.sort_values(
    by=[
        "_priority_rank",
        "_relevance_rank",
        "narrative_hit_count",
        "global_score",
        "topic_observations",
        "unique_dipoles_in_analyzer",
        "avg_pi",
    ],
    ascending=[False, False, False, False, False, False, False]
).reset_index(drop=True)

details_df = details_df.drop(columns=["_priority_rank", "_relevance_rank"])


# =========================================================
# SAVE
# =========================================================
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
details_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε η δημιουργία του high-priority topic details table.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_CSV}")
print(f"Σύνολο επιλεγμένων topics: {len(details_df)}")

print("\nΚατανομή candidate_narrative:")
print(details_df["candidate_narrative"].value_counts(dropna=False))

print("\nTop 15 topics:")
preview_cols = [
    "topic_id",
    "candidate_narrative",
    "relevance_label",
    "priority_for_review",
    "global_score",
    "topic_observations",
    "unique_dipoles_in_analyzer",
    "top_phrases"
]
print(details_df[preview_cols].head(15).to_string(index=False))