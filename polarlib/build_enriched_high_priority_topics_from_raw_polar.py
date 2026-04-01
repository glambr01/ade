import os
import re
import json
import pickle
import ast
from collections import Counter

import pandas as pd


# =========================================================
# CONFIG
# =========================================================
BASE_PATH = "results"

TOPIC_CLUSTERS_CSV = os.path.join(BASE_PATH, "analysis_results", "final_topic_clusters.csv")
ANALYZER_RESULTS_CSV = os.path.join(BASE_PATH, "analysis_results", "analyzer_results.csv")
GROUP_TOPIC_COHESION_CSV = os.path.join(BASE_PATH, "analysis_results", "group_topic_cohesion.csv")

TOPICS_JSON = os.path.join(BASE_PATH, "topics.json")
FELLOWSHIPS_JSON = os.path.join(BASE_PATH, "polarization", "fellowships.json")
DIPOLES_PCKL = os.path.join(BASE_PATH, "polarization", "dipoles.pckl")

OUTPUT_CSV = os.path.join(
    BASE_PATH,
    "analysis_results",
    "enriched_high_priority_topics_from_raw_polar.csv"
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


def top_items_with_counts(items, limit=10):
    cleaned = [normalize_text(x) for x in items if normalize_text(x)]
    counts = Counter(cleaned)
    return [f"{item} ({count})" for item, count in counts.most_common(limit)]


def stringify_top(items, limit=10):
    return " | ".join(top_items_with_counts(items, limit=limit))


def clean_duplicate_columns(df):
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_pickle(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def parse_dipole_id(dipole_id):
    """
    D179_910 -> (179, 910)
    """
    dipole_id = normalize_text(dipole_id)
    if not dipole_id.startswith("D"):
        return None

    core = dipole_id[1:]
    parts = core.split("_")
    if len(parts) != 2:
        return None

    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def get_topic_phrases(topic_id, topics_data, top_n=10):
    topic_value = topics_data.get(topic_id)
    if topic_value is None:
        return []

    if isinstance(topic_value, dict):
        noun_phrases = topic_value.get("noun_phrases", [])
        if isinstance(noun_phrases, list):
            return [normalize_text(x) for x in noun_phrases[:top_n] if normalize_text(x)]

        vals = []
        for _, v in topic_value.items():
            if isinstance(v, list):
                vals.extend([normalize_text(x) for x in v if normalize_text(x)])
            else:
                vals.append(normalize_text(v))
        return vals[:top_n]

    if isinstance(topic_value, list):
        return [normalize_text(x) for x in topic_value[:top_n] if normalize_text(x)]

    return [normalize_text(topic_value)] if normalize_text(topic_value) else []


def build_dipole_lookup(dipoles_raw):
    """
    dipoles.pckl expected structure:
    [
      ((179, 910), {meta}),
      ...
    ]
    """
    lookup = {}

    for item in dipoles_raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue

        pair, meta = item
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue

        try:
            a, b = int(pair[0]), int(pair[1])
        except Exception:
            continue

        key = f"D{a}_{b}"
        lookup[key] = {
            "pair": (a, b),
            "meta": meta if isinstance(meta, dict) else {}
        }

    return lookup


def get_fellowship_members(fellowships, fellowship_id, top_n=20):
    if isinstance(fellowships, list):
        if 0 <= fellowship_id < len(fellowships):
            members = fellowships[fellowship_id]
            if isinstance(members, list):
                return [normalize_text(x) for x in members[:top_n] if normalize_text(x)]
            return [normalize_text(members)] if normalize_text(members) else []

    return []


# =========================================================
# LOAD
# =========================================================
required_files = [
    TOPIC_CLUSTERS_CSV,
    ANALYZER_RESULTS_CSV,
    GROUP_TOPIC_COHESION_CSV,
    TOPICS_JSON,
    FELLOWSHIPS_JSON,
    DIPOLES_PCKL,
]

for path in required_files:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {path}")

topics_df = pd.read_csv(TOPIC_CLUSTERS_CSV)
topics_df = clean_duplicate_columns(topics_df)

analyzer_df = pd.read_csv(ANALYZER_RESULTS_CSV)
analyzer_df = clean_duplicate_columns(analyzer_df)

group_topic_df = pd.read_csv(GROUP_TOPIC_COHESION_CSV)
group_topic_df = clean_duplicate_columns(group_topic_df)

topics_data = load_json(TOPICS_JSON)
fellowships_data = load_json(FELLOWSHIPS_JSON)
dipoles_raw = load_pickle(DIPOLES_PCKL)

if isinstance(fellowships_data, dict) and "fellowships" in fellowships_data:
    fellowships = fellowships_data["fellowships"]
else:
    fellowships = fellowships_data

dipole_lookup = build_dipole_lookup(dipoles_raw)


# =========================================================
# VALIDATION
# =========================================================
required_topic_cols = {
    "topic_id",
    "topic_cluster",
    "topic_subcluster",
    "cluster_role",
    "candidate_narrative",
    "relevance_label",
    "priority_for_review",
    "global_score",
    "topic_observations",
}
missing_topic = required_topic_cols - set(topics_df.columns)
if missing_topic:
    raise ValueError(f"Λείπουν στήλες από final_topic_clusters.csv: {sorted(missing_topic)}")

required_analyzer_cols = {"topic", "dipole", "pi", "obs", "label"}
missing_analyzer = required_analyzer_cols - set(analyzer_df.columns)
if missing_analyzer:
    raise ValueError(f"Λείπουν στήλες από analyzer_results.csv: {sorted(missing_analyzer)}")

required_group_cols = {"topic", "fellowship"}
missing_group = required_group_cols - set(group_topic_df.columns)
if missing_group:
    raise ValueError(f"Λείπουν στήλες από group_topic_cohesion.csv: {sorted(missing_group)}")


# =========================================================
# PREP
# =========================================================
topics_df["topic_id"] = topics_df["topic_id"].astype(str)

analyzer_df["topic"] = analyzer_df["topic"].astype(str)
analyzer_df["dipole"] = analyzer_df["dipole"].astype(str)
analyzer_df["pi_num"] = pd.to_numeric(analyzer_df["pi"], errors="coerce")
analyzer_df["obs_num"] = pd.to_numeric(analyzer_df["obs"], errors="coerce")

group_topic_df["topic"] = group_topic_df["topic"].astype(str)
group_topic_df["fellowship"] = group_topic_df["fellowship"].astype(str)

selected_topic_ids = set(topics_df["topic_id"].tolist())


# =========================================================
# BUILD ENRICHED ROWS
# =========================================================
rows = []

for _, trow in topics_df.iterrows():
    topic_id = str(trow["topic_id"])

    # ---------- topic phrases directly from topics.json ----------
    raw_topic_phrases = get_topic_phrases(topic_id, topics_data, top_n=10)

    # ---------- analyzer ----------
    an = analyzer_df[analyzer_df["topic"] == topic_id].copy()

    dipole_ids = an["dipole"].dropna().astype(str).tolist()
    analyzer_labels = an["label"].dropna().astype(str).tolist()

    pi_vals = an["pi_num"].dropna()
    obs_vals = an["obs_num"].dropna()

    # ---------- group_topic_cohesion ----------
    gt = group_topic_df[group_topic_df["topic"] == topic_id].copy()
    gt_fellowships = gt["fellowship"].dropna().astype(str).tolist()

    gt_summary_items = []
    for _, grow in gt.iterrows():
        gt_summary_items.append(
            f"{grow['fellowship']} (cohesiveness={grow.get('cohesiveness', '')}, member_size={grow.get('member_size', '')}, member_ratio={grow.get('member_ratio', '')})"
        )
    gt_summary = " | ".join(gt_summary_items[:10])

    # ---------- raw dipole enrichment ----------
    dipole_entities_side_1 = []
    dipole_entities_side_2 = []
    dipole_fellowship_ids = []
    dipole_positive_ratios = []
    dipole_negative_ratios = []
    dipole_pos_counts = []
    dipole_neg_counts = []

    for d_id in dipole_ids:
        d_info = dipole_lookup.get(d_id)

        # fallback: try parsing the id if not found directly
        if d_info is None:
            pair = parse_dipole_id(d_id)
            if pair is not None:
                alt_key = f"D{pair[0]}_{pair[1]}"
                d_info = dipole_lookup.get(alt_key)

        if d_info is None:
            continue

        pair = d_info.get("pair", ())
        meta = d_info.get("meta", {})

        if len(pair) == 2:
            f1, f2 = pair
            dipole_fellowship_ids.extend([f"F{f1}", f"F{f2}"])

        simap_1 = meta.get("simap_1", [])
        simap_2 = meta.get("simap_2", [])

        if isinstance(simap_1, list):
            dipole_entities_side_1.extend([normalize_text(x) for x in simap_1 if normalize_text(x)])
        if isinstance(simap_2, list):
            dipole_entities_side_2.extend([normalize_text(x) for x in simap_2 if normalize_text(x)])

        if meta.get("positive_ratio") is not None:
            dipole_positive_ratios.append(meta.get("positive_ratio"))
        if meta.get("negative_ratio") is not None:
            dipole_negative_ratios.append(meta.get("negative_ratio"))
        if meta.get("pos") is not None:
            dipole_pos_counts.append(meta.get("pos"))
        if meta.get("neg") is not None:
            dipole_neg_counts.append(meta.get("neg"))

    # ---------- fellowships + members ----------
    all_fellowships = gt_fellowships + dipole_fellowship_ids
    unique_fellowships = []
    for x in all_fellowships:
        x = normalize_text(x)
        if x and x not in unique_fellowships:
            unique_fellowships.append(x)

    fellowship_members_summary_items = []
    for f in unique_fellowships[:10]:
        f_num = None
        match = re.search(r"(\d+)", f)
        if match:
            f_num = int(match.group(1))

        members = get_fellowship_members(fellowships, f_num, top_n=10) if f_num is not None else []
        if members:
            fellowship_members_summary_items.append(f"{f}: {', '.join(members[:5])}")

    fellowship_members_summary = " | ".join(fellowship_members_summary_items[:10])

    # ---------- combined entities ----------
    all_entities = dipole_entities_side_1 + dipole_entities_side_2

    rows.append({
        # identity / clustering
        "topic_id": topic_id,
        "topic_cluster": trow.get("topic_cluster", ""),
        "topic_subcluster": trow.get("topic_subcluster", ""),
        "cluster_role": trow.get("cluster_role", ""),
        "candidate_narrative": trow.get("candidate_narrative", ""),
        "relevance_label": trow.get("relevance_label", ""),
        "priority_for_review": trow.get("priority_for_review", ""),

        # topic-level metrics
        "global_score": trow.get("global_score", ""),
        "topic_observations": trow.get("topic_observations", ""),
        "top_phrases_from_cluster_file": trow.get("top_phrases", ""),
        "top_phrases_from_topics_json": " | ".join(raw_topic_phrases),

        # analyzer summaries
        "analyzer_rows": len(an),
        "related_dipoles_count": len(set([normalize_text(x) for x in dipole_ids if normalize_text(x)])),
        "related_dipoles": stringify_top(dipole_ids, limit=10),
        "top_labels_from_analyzer": stringify_top(analyzer_labels, limit=10),
        "avg_pi_from_analyzer": float(pi_vals.mean()) if not pi_vals.empty else None,
        "max_pi_from_analyzer": float(pi_vals.max()) if not pi_vals.empty else None,
        "sum_obs_in_analyzer": int(obs_vals.sum()) if not obs_vals.empty else 0,
        "max_obs_in_analyzer": int(obs_vals.max()) if not obs_vals.empty else 0,

        # fellowship summaries
        "related_fellowships_count": len(unique_fellowships),
        "related_fellowships": stringify_top(unique_fellowships, limit=10),
        "group_topic_rows_count": len(gt),
        "group_topic_fellowships": stringify_top(gt_fellowships, limit=10),
        "group_topic_summary": gt_summary,
        "fellowship_members_summary": fellowship_members_summary,

        # entity summaries from raw dipoles
        "top_entities": stringify_top(all_entities, limit=12),
        "top_side_1_entities": stringify_top(dipole_entities_side_1, limit=8),
        "top_side_2_entities": stringify_top(dipole_entities_side_2, limit=8),

        # dipole structural signals
        "avg_positive_ratio_from_dipoles": (
            sum([safe_float(x) for x in dipole_positive_ratios]) / len(dipole_positive_ratios)
            if dipole_positive_ratios else None
        ),
        "avg_negative_ratio_from_dipoles": (
            sum([safe_float(x) for x in dipole_negative_ratios]) / len(dipole_negative_ratios)
            if dipole_negative_ratios else None
        ),
        "sum_pos_edges_from_dipoles": sum([safe_int(x) for x in dipole_pos_counts]) if dipole_pos_counts else 0,
        "sum_neg_edges_from_dipoles": sum([safe_int(x) for x in dipole_neg_counts]) if dipole_neg_counts else 0,

        # manual interpretation fields
        "keep_for_final_analysis": "",
        "main_actor_entities": "",
        "main_fellowship_interpretation": "",
        "final_interpretive_notes": "",
    })

enriched_df = pd.DataFrame(rows)
enriched_df = clean_duplicate_columns(enriched_df)


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
role_rank = {"core": 3, "supporting": 2, "secondary": 1, "review": 0}

enriched_df["_priority_rank"] = enriched_df["priority_for_review"].map(priority_rank).fillna(0)
enriched_df["_relevance_rank"] = enriched_df["relevance_label"].map(relevance_rank).fillna(0)
enriched_df["_role_rank"] = enriched_df["cluster_role"].map(role_rank).fillna(0)

enriched_df = enriched_df.sort_values(
    by=[
        "topic_cluster",
        "_role_rank",
        "_priority_rank",
        "_relevance_rank",
        "global_score",
        "topic_observations",
        "related_dipoles_count",
        "related_fellowships_count",
    ],
    ascending=[True, False, False, False, False, False, False, False]
).reset_index(drop=True)

enriched_df = enriched_df.drop(columns=["_priority_rank", "_relevance_rank", "_role_rank"])


# =========================================================
# SAVE
# =========================================================
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
enriched_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε η δημιουργία του enriched high-priority topics CSV από raw POLAR outputs.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_CSV}")
print(f"Σύνολο topics: {len(enriched_df)}")

print("\nΚατανομή topic clusters:")
print(enriched_df["topic_cluster"].value_counts(dropna=False))

print("\nPreview:")
preview_cols = [
    "topic_id",
    "topic_cluster",
    "candidate_narrative",
    "global_score",
    "related_dipoles_count",
    "related_fellowships_count",
    "top_entities",
    "top_phrases_from_topics_json",
]
print(enriched_df[preview_cols].head(15).to_string(index=False))