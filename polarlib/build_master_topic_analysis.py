import os
import json
import re
from collections import Counter

import pandas as pd


# =========================================================
# CONFIG
# =========================================================
RESULTS_DIR = "results"
GLOBAL_POLARIZATION_CSV = os.path.join(RESULTS_DIR, "analysis_results", "global_polarization.csv")
ANALYZER_RESULTS_CSV = os.path.join(RESULTS_DIR, "analysis_results", "analyzer_results.csv")
TOPICS_JSON = os.path.join(RESULTS_DIR, "topics.json")

OUTPUT_CSV = os.path.join(RESULTS_DIR, "analysis_results", "master_topic_analysis.csv")


# =========================================================
# HELPERS
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def unique_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        item = normalize_text(item)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def top_unique_strings(items, limit=8):
    cleaned = [normalize_text(x) for x in items if normalize_text(x)]
    return unique_preserve_order(cleaned)[:limit]


def safe_get_topic_phrases(topic_value, limit=10):
    """
    Το topics.json σου έχει δομή τύπου:
    {
      "t123": {
         "noun_phrases": [...]
      },
      ...
    }
    """
    if isinstance(topic_value, dict):
        noun_phrases = topic_value.get("noun_phrases", [])
        if isinstance(noun_phrases, list):
            phrases = top_unique_strings(noun_phrases, limit=limit)
            return phrases

        # fallback αν έχει άλλη δομή
        all_values = []
        for _, v in topic_value.items():
            if isinstance(v, list):
                all_values.extend([str(x) for x in v])
            else:
                all_values.append(str(v))
        return top_unique_strings(all_values, limit=limit)

    elif isinstance(topic_value, list):
        return top_unique_strings(topic_value, limit=limit)

    elif topic_value is None:
        return []

    return [normalize_text(str(topic_value))]


def summarize_labels(labels, limit=5):
    if not labels:
        return []
    counts = Counter(labels)
    return [f"{label} ({count})" for label, count in counts.most_common(limit)]


def summarize_dipoles(dipoles, limit=5):
    if not dipoles:
        return []
    counts = Counter(dipoles)
    return [f"{dipole} ({count})" for dipole, count in counts.most_common(limit)]


# =========================================================
# LOAD DATA
# =========================================================
if not os.path.exists(GLOBAL_POLARIZATION_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {GLOBAL_POLARIZATION_CSV}")

if not os.path.exists(ANALYZER_RESULTS_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {ANALYZER_RESULTS_CSV}")

if not os.path.exists(TOPICS_JSON):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {TOPICS_JSON}")

global_df = pd.read_csv(GLOBAL_POLARIZATION_CSV)
analyzer_df = pd.read_csv(ANALYZER_RESULTS_CSV)

with open(TOPICS_JSON, "r", encoding="utf-8") as f:
    topics_data = json.load(f)


# =========================================================
# VALIDATE EXPECTED COLUMNS
# =========================================================
required_global_cols = {"topic", "dt", "obst", "mt", "score"}
required_analyzer_cols = {"topic", "dipole", "pi", "obs", "label"}

missing_global = required_global_cols - set(global_df.columns)
missing_analyzer = required_analyzer_cols - set(analyzer_df.columns)

if missing_global:
    raise ValueError(f"Λείπουν στήλες από global_polarization.csv: {sorted(missing_global)}")

if missing_analyzer:
    raise ValueError(f"Λείπουν στήλες από analyzer_results.csv: {sorted(missing_analyzer)}")


# =========================================================
# BUILD ANALYZER AGGREGATES BY TOPIC
# =========================================================
topic_aggregates = {}

grouped = analyzer_df.groupby("topic", dropna=False)

for topic_id, group in grouped:
    topic_id = str(topic_id)

    dipoles = [str(x) for x in group["dipole"].dropna().tolist()]
    labels = [str(x) for x in group["label"].dropna().tolist()]

    pi_values = pd.to_numeric(group["pi"], errors="coerce").dropna()
    obs_values = pd.to_numeric(group["obs"], errors="coerce").dropna()

    topic_aggregates[topic_id] = {
        "num_dipoles": int(group["dipole"].nunique(dropna=True)),
        "analyzer_rows": int(len(group)),
        "top_dipoles": summarize_dipoles(dipoles, limit=5),
        "top_labels": summarize_labels(labels, limit=5),
        "avg_pi": float(pi_values.mean()) if not pi_values.empty else None,
        "max_pi": float(pi_values.max()) if not pi_values.empty else None,
        "sum_obs": int(obs_values.sum()) if not obs_values.empty else 0,
        "max_obs": int(obs_values.max()) if not obs_values.empty else 0,
    }


# =========================================================
# BUILD MASTER TABLE
# =========================================================
rows = []

for _, row in global_df.iterrows():
    topic_id = str(row["topic"])

    topic_value = topics_data.get(topic_id)
    topic_phrases = safe_get_topic_phrases(topic_value, limit=10)

    agg = topic_aggregates.get(topic_id, {})

    rows.append({
        "topic_id": topic_id,

        # from global_polarization.csv
        "global_score": row["score"],
        "topic_observations": row["obst"],
        "dt": row["dt"],
        "mt": row["mt"],
        "maxm": row.get("maxm"),
        "minm": row.get("minm"),
        "stdm": row.get("stdm"),
        "avgm": row.get("avgm"),

        # from topics.json
        "top_phrases": " | ".join(topic_phrases),
        "phrase_count_shown": len(topic_phrases),

        # from analyzer_results.csv
        "num_dipoles": agg.get("num_dipoles", 0),
        "analyzer_rows": agg.get("analyzer_rows", 0),
        "top_dipoles": " | ".join(agg.get("top_dipoles", [])),
        "top_labels": " | ".join(agg.get("top_labels", [])),
        "avg_pi": agg.get("avg_pi"),
        "max_pi": agg.get("max_pi"),
        "sum_obs_in_analyzer": agg.get("sum_obs", 0),
        "max_obs_in_analyzer": agg.get("max_obs", 0),

        # placeholders for your later annotation
        "candidate_narrative": "",
        "relevant_to_disinformation": "",
        "notes": ""
    })

master_df = pd.DataFrame(rows)


# =========================================================
# SORT
# =========================================================
master_df = master_df.sort_values(
    by=["global_score", "topic_observations", "num_dipoles", "avg_pi"],
    ascending=[False, False, False, False]
).reset_index(drop=True)


# =========================================================
# SAVE
# =========================================================
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
master_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε η δημιουργία του master topic analysis table.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_CSV}")
print(f"Σύνολο topics: {len(master_df)}")

print("\nTop 10 topics:")
preview_cols = ["topic_id", "global_score", "topic_observations", "num_dipoles", "top_phrases"]
print(master_df[preview_cols].head(10).to_string(index=False))