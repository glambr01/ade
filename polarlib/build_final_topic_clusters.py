import os
import re
from collections import Counter

import pandas as pd


# =========================================================
# CONFIG
# =========================================================
RESULTS_DIR = "results"
INPUT_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "high_priority_topic_details.csv"
)

OUTPUT_TOPIC_CLUSTERS_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "final_topic_clusters.csv"
)

OUTPUT_CLUSTER_SUMMARY_CSV = os.path.join(
    RESULTS_DIR, "analysis_results", "final_topic_cluster_summary.csv"
)


# =========================================================
# HELPERS
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    text = str(text).lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_pipe_text(text):
    if text is None:
        return []
    parts = [x.strip() for x in str(text).split("|")]
    return [p for p in parts if p]


def top_items_with_counts(items, limit=12):
    cleaned = [str(x).strip() for x in items if str(x).strip()]
    counts = Counter(cleaned)
    return [f"{item} ({count})" for item, count in counts.most_common(limit)]


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


# =========================================================
# LOAD
# =========================================================
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV)

required_cols = {
    "topic_id",
    "candidate_narrative",
    "relevance_label",
    "priority_for_review",
    "global_score",
    "topic_observations",
    "top_phrases",
}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Λείπουν στήλες από high_priority_topic_details.csv: {sorted(missing)}")


# =========================================================
# CLUSTER RULES
# =========================================================
def assign_cluster(row):
    topic_id = str(row.get("topic_id", ""))
    narrative = normalize_text(row.get("candidate_narrative", ""))
    phrases = normalize_text(row.get("top_phrases", ""))

    # -------- Core cluster A: Gender / eligibility / identity --------
    if narrative == "gender controversy":
        if any(x in phrases for x in [
            "eligibility", "gender eligibility", "sex development",
            "transgender", "gender identity", "athlete's gender",
            "women's competition", "first female", "female"
        ]):
            return pd.Series({
                "topic_cluster": "Cluster A - Gender / eligibility / identity",
                "cluster_role": "core"
            })
        return pd.Series({
            "topic_cluster": "Cluster A - Gender / eligibility / identity",
            "cluster_role": "supporting"
        })

    # -------- Core cluster B: Opening ceremony / Christianity --------
    if narrative == "opening ceremony / christianity controversy":
        return pd.Series({
            "topic_cluster": "Cluster B - Opening ceremony / Christianity",
            "cluster_role": "core"
        })

    # -------- Secondary cluster C --------
    if narrative in {
        "political/geopolitical",
        "anti-lgbtq framing",
        "doping/cheating"
    }:
        return pd.Series({
            "topic_cluster": "Cluster C - Secondary controversial themes",
            "cluster_role": "secondary"
        })

    # fallback
    return pd.Series({
        "topic_cluster": "Cluster D - Unassigned / review",
        "cluster_role": "review"
    })


cols_to_replace = [
    "topic_cluster",
    "cluster_role",
    "topic_subcluster"
]

existing = [c for c in cols_to_replace if c in df.columns]
if existing:
    df = df.drop(columns=existing)

clustered = df.apply(assign_cluster, axis=1)
topic_clusters_df = pd.concat([df, clustered], axis=1)

# =========================================================
# OPTIONAL FINE-GRAINED SUBCLUSTERS
# =========================================================
def assign_subcluster(row):
    narrative = normalize_text(row.get("candidate_narrative", ""))
    phrases = normalize_text(row.get("top_phrases", ""))

    if narrative == "gender controversy":
        if any(x in phrases for x in [
            "transgender", "trans athletes", "nonbinary", "gender identity"
        ]):
            return "A1 - Transgender / identity"
        if any(x in phrases for x in [
            "eligibility", "sex development", "female", "women's competition"
        ]):
            return "A2 - Eligibility / sex classification"
        if "kaylia nemour" in phrases:
            return "A3 - Athlete-specific case"
        return "A0 - General gender discourse"

    if narrative == "opening ceremony / christianity controversy":
        if any(x in phrases for x in [
            "last supper", "religious iconography", "religious imagery"
        ]):
            return "B1 - Religious iconography"
        return "B0 - Mockery / offense discourse"

    if narrative == "political/geopolitical":
        return "C1 - Security / terrorism / geopolitics"

    if narrative == "anti-lgbtq framing":
        return "C2 - Anti-LGBTQ framing"

    if narrative == "doping/cheating":
        return "C3 - Doping / drugs"

    return "D0 - Review"


topic_clusters_df["topic_subcluster"] = topic_clusters_df.apply(assign_subcluster, axis=1)


# =========================================================
# SORT TOPIC-LEVEL OUTPUT
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
role_rank = {"core": 3, "supporting": 2, "secondary": 1, "review": 0}

topic_clusters_df["_priority_rank"] = topic_clusters_df["priority_for_review"].map(priority_rank).fillna(0)
topic_clusters_df["_relevance_rank"] = topic_clusters_df["relevance_label"].map(relevance_rank).fillna(0)
topic_clusters_df["_role_rank"] = topic_clusters_df["cluster_role"].map(role_rank).fillna(0)

topic_clusters_df = topic_clusters_df.sort_values(
    by=[
        "topic_cluster",
        "_role_rank",
        "_priority_rank",
        "_relevance_rank",
        "global_score",
        "topic_observations"
    ],
    ascending=[True, False, False, False, False, False]
).reset_index(drop=True)

topic_clusters_df = topic_clusters_df.drop(
    columns=["_priority_rank", "_relevance_rank", "_role_rank"]
)


# =========================================================
# BUILD CLUSTER SUMMARY
# =========================================================
summary_rows = []

grouped = topic_clusters_df.groupby("topic_cluster", dropna=False)

for cluster_name, group in grouped:
    topic_ids = group["topic_id"].astype(str).tolist()
    phrases = []
    for val in group["top_phrases"].fillna("").tolist():
        phrases.extend(split_pipe_text(val))

    narratives = group["candidate_narrative"].fillna("").astype(str).tolist()
    subclusters = group["topic_subcluster"].fillna("").astype(str).tolist()

    summary_rows.append({
        "topic_cluster": cluster_name,
        "topic_count": len(group),
        "topic_ids": " | ".join(topic_ids),
        "core_topics_count": int((group["cluster_role"] == "core").sum()),
        "supporting_topics_count": int((group["cluster_role"] == "supporting").sum()),
        "secondary_topics_count": int((group["cluster_role"] == "secondary").sum()),
        "sum_global_score": round(group["global_score"].apply(safe_float).sum(), 6),
        "avg_global_score": round(group["global_score"].apply(safe_float).mean(), 6),
        "sum_topic_observations": int(group["topic_observations"].apply(safe_int).sum()),
        "avg_topic_observations": round(group["topic_observations"].apply(safe_float).mean(), 2),
        "top_candidate_narratives": " | ".join(top_items_with_counts(narratives, limit=6)),
        "top_subclusters": " | ".join(top_items_with_counts(subclusters, limit=6)),
        "representative_phrases": " | ".join(top_items_with_counts(phrases, limit=15)),
    })

cluster_summary_df = pd.DataFrame(summary_rows)

cluster_summary_df = cluster_summary_df.sort_values(
    by=["topic_count", "sum_global_score", "sum_topic_observations"],
    ascending=[False, False, False]
).reset_index(drop=True)


# =========================================================
# SAVE
# =========================================================
os.makedirs(os.path.dirname(OUTPUT_TOPIC_CLUSTERS_CSV), exist_ok=True)

topic_clusters_df.to_csv(OUTPUT_TOPIC_CLUSTERS_CSV, index=False, encoding="utf-8-sig")
cluster_summary_df.to_csv(OUTPUT_CLUSTER_SUMMARY_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε η δημιουργία των τελικών thematic clusters.")
print(f"Αποθηκεύτηκε topic-level αρχείο εδώ: {OUTPUT_TOPIC_CLUSTERS_CSV}")
print(f"Αποθηκεύτηκε summary αρχείο εδώ: {OUTPUT_CLUSTER_SUMMARY_CSV}")

print("\nΚατανομή topic clusters:")
print(topic_clusters_df["topic_cluster"].value_counts(dropna=False))

print("\nCluster summary:")
preview_cols = [
    "topic_cluster",
    "topic_count",
    "sum_global_score",
    "sum_topic_observations",
    "top_candidate_narratives"
]
print(cluster_summary_df[preview_cols].to_string(index=False))