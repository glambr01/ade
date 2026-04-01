import os
import re
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
RESULTS_DIR = "results"
INPUT_CSV = os.path.join(RESULTS_DIR, "analysis_results", "master_topic_analysis.csv")
OUTPUT_CSV = os.path.join(RESULTS_DIR, "analysis_results", "refined_master_topic_analysis.csv")


# =========================================================
# HELPERS
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    text = str(text).lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_phrases(text):
    if text is None:
        return []
    parts = [normalize_text(x) for x in str(text).split("|")]
    return [p for p in parts if p]


def count_matches(text, keywords):
    hits = []
    for kw in keywords:
        kw_norm = normalize_text(kw)
        if kw_norm and kw_norm in text:
            hits.append(kw)
    return list(dict.fromkeys(hits))


def has_any(text, keywords):
    return len(count_matches(text, keywords)) > 0


# =========================================================
# KEYWORD SETS
# =========================================================
GENERIC_KEYWORDS = [
    "the games", "games", "game", "medal", "medals", "gold", "silver", "bronze",
    "athlete", "athletes", "olympic athlete", "event", "events", "world",
    "scene", "scenes", "sport", "sports", "athletics", "team", "teams",
    "competition", "match", "matches", "win", "won", "loss", "final",
    "opening and closing ceremonies", "opening and closing ceremony"
]

GENERIC_SHORT_PHRASES = {
    "gold", "medal", "medals", "the games", "games", "athletics",
    "the world", "world", "event", "events", "scene", "scenes"
}

CATEGORY_KEYWORDS = {
    "gender controversy": [
        "imane khelif", "khelif", "boxing", "boxer", "women boxing",
        "female boxer", "gender", "sex", "intersex", "eligibility",
        "trans", "transgender", "male", "female", "algeria", "algerian"
    ],
    "opening ceremony / christianity controversy": [
        "opening ceremony", "last supper", "christianity", "christian",
        "jesus", "apostles", "religion", "religious", "mock", "mockery",
        "blasphemy", "parody", "faith", "biblical", "offend", "offensive"
    ],
    "political/geopolitical": [
        "russia", "ukraine", "israel", "palestine", "france", "algeria",
        "government", "state", "propaganda", "boycott", "political", "geopolitical"
    ],
    "security / sabotage / fear": [
        "security", "threat", "attack", "terror", "terrorism",
        "sabotage", "risk", "danger", "unsafe", "violence"
    ],
    "doping/cheating": [
        "doping", "cheating", "drug", "drugs", "ban",
        "suspended", "violation", "performance enhancing", "tested positive"
    ],
    "anti-France / anti-Paris": [
        "paris", "france", "french", "seine", "dirty seine",
        "paris unsafe", "paris dirty", "anti france", "anti french"
    ],
    "anti-LGBTQ framing": [
        "lgbt", "lgbtq", "drag", "queer", "gay",
        "woke", "gender ideology", "rainbow"
    ],
}

LOW_SIGNAL_KEYWORDS = [
    "one event", "the scene", "the world", "gold", "a medal",
    "olympic athletes", "athletic", "some athletes", "their games"
]


# =========================================================
# LOAD
# =========================================================
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV)

required_cols = {"topic_id", "global_score", "topic_observations", "top_phrases", "num_dipoles"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Λείπουν στήλες από master_topic_analysis.csv: {sorted(missing)}")


# =========================================================
# CLASSIFICATION
# =========================================================
def classify_topic(row):
    topic_id = row["topic_id"]
    phrases = split_phrases(row.get("top_phrases", ""))
    joined_text = normalize_text(" ".join(phrases))

    try:
        global_score = float(row.get("global_score", 0))
    except Exception:
        global_score = 0.0

    try:
        observations = float(row.get("topic_observations", 0))
    except Exception:
        observations = 0.0

    try:
        num_dipoles = float(row.get("num_dipoles", 0))
    except Exception:
        num_dipoles = 0.0

    # -----------------------------------------------------
    # Generic detection
    # -----------------------------------------------------
    generic_hits = count_matches(joined_text, GENERIC_KEYWORDS)
    low_signal_hits = count_matches(joined_text, LOW_SIGNAL_KEYWORDS)

    short_generic_phrase_count = sum(1 for p in phrases if p in GENERIC_SHORT_PHRASES)
    total_phrase_count = len(phrases)

    is_generic = False
    generic_reason = ""

    if total_phrase_count > 0 and short_generic_phrase_count >= min(4, total_phrase_count):
        is_generic = True
        generic_reason = "many short generic phrases"
    elif len(generic_hits) >= 4:
        is_generic = True
        generic_reason = "many generic keyword hits"
    elif len(low_signal_hits) >= 2 and global_score > 10:
        is_generic = True
        generic_reason = "high score but semantically weak phrases"

    # -----------------------------------------------------
    # Narrative classification
    # -----------------------------------------------------
    category_hits = {}
    trigger_keywords = []

    for category, keywords in CATEGORY_KEYWORDS.items():
        hits = count_matches(joined_text, keywords)
        if hits:
            category_hits[category] = hits
            trigger_keywords.extend(hits)

    trigger_keywords = list(dict.fromkeys(trigger_keywords))

    candidate_narrative = "unclear"
    max_hits = 0

    for category, hits in category_hits.items():
        if len(hits) > max_hits:
            candidate_narrative = category
            max_hits = len(hits)

    # -----------------------------------------------------
    # Relevance rules
    # -----------------------------------------------------
    strong_structure = (
        global_score >= 5 or
        observations >= 50 or
        num_dipoles >= 5
    )

    very_strong_structure = (
        global_score >= 10 or
        observations >= 200 or
        num_dipoles >= 15
    )

    if is_generic and candidate_narrative == "unclear":
        relevance_label = "generic/noisy"
        priority_for_review = "low"

    elif candidate_narrative != "unclear":
        if max_hits >= 3 and very_strong_structure:
            relevance_label = "strong_candidate"
            priority_for_review = "high"
        elif max_hits >= 2 and strong_structure:
            relevance_label = "candidate"
            priority_for_review = "high"
        elif max_hits >= 1:
            relevance_label = "possible_candidate"
            priority_for_review = "medium"
        else:
            relevance_label = "possible_candidate"
            priority_for_review = "medium"

    else:
        if is_generic:
            relevance_label = "generic/noisy"
            priority_for_review = "low"
        elif strong_structure:
            relevance_label = "needs_manual_review"
            priority_for_review = "medium"
        else:
            relevance_label = "low_signal"
            priority_for_review = "low"

    # extra boost for known target narratives
    if candidate_narrative in {
        "gender controversy",
        "opening ceremony / christianity controversy"
    } and max_hits >= 2:
        priority_for_review = "high"

    return pd.Series({
        "is_generic_topic": "yes" if is_generic else "no",
        "generic_reason": generic_reason,
        "generic_hits": ", ".join(generic_hits),
        "candidate_narrative": candidate_narrative,
        "narrative_hit_count": max_hits,
        "trigger_keywords": ", ".join(trigger_keywords),
        "relevance_label": relevance_label,
        "priority_for_review": priority_for_review
    })

for col in [
    "candidate_narrative",
    "relevant_to_disinformation",
    "notes",
    "is_generic_topic",
    "generic_reason",
    "generic_hits",
    "narrative_hit_count",
    "trigger_keywords",
    "relevance_label",
    "priority_for_review"
]:
    if col in df.columns:
        df = df.drop(columns=[col])

classified = df.apply(classify_topic, axis=1)
refined_df = pd.concat([df, classified], axis=1)


# =========================================================
# SORTING
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

refined_df["_priority_rank"] = refined_df["priority_for_review"].map(priority_rank).fillna(0)
refined_df["_relevance_rank"] = refined_df["relevance_label"].map(relevance_rank).fillna(0)

refined_df = refined_df.sort_values(
    by=["_priority_rank", "_relevance_rank", "narrative_hit_count", "global_score", "topic_observations", "num_dipoles"],
    ascending=[False, False, False, False, False, False]
).reset_index(drop=True)

refined_df = refined_df.drop(columns=["_priority_rank", "_relevance_rank"])


# =========================================================
# SAVE
# =========================================================
refined_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε το refined master topic analysis.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_CSV}")
print(f"Σύνολο topics: {len(refined_df)}")

print("\nΚατανομή relevance_label:")
print(refined_df["relevance_label"].value_counts(dropna=False))

print("\nΚατανομή candidate_narrative (top 15):")
print(refined_df["candidate_narrative"].value_counts(dropna=False).head(15))

print("\nTop 15 high-priority topics:")
preview_cols = [
    "topic_id", "global_score", "topic_observations", "num_dipoles",
    "candidate_narrative", "relevance_label", "priority_for_review", "top_phrases"
]
print(refined_df[preview_cols].head(15).to_string(index=False))