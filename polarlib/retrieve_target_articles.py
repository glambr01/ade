import os
import re
import json
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
INPUT_JSON = "dataset.json" 
OUTPUT_DIR = "results"

IMANE_OUTPUT = os.path.join(OUTPUT_DIR, "articles_imane_khelif.csv")
CEREMONY_OUTPUT = os.path.join(OUTPUT_DIR, "articles_opening_ceremony_christianity.csv")


# =========================================================
# HELPERS
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    text = str(text).lower()
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"[^a-z0-9\s\-_'/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_first_existing(article, candidates, default=""):
    for key in candidates:
        if key in article and article[key] is not None:
            return article[key]
    return default


def count_keyword_hits(text, keywords):
    hits = []
    for kw in keywords:
        kw_norm = normalize_text(kw)
        if kw_norm and kw_norm in text:
            hits.append(kw)
    return list(dict.fromkeys(hits))


def make_snippet(original_text, matched_keywords, window=250):
    if not original_text:
        return ""

    low = original_text.lower()

    first_pos = None
    matched_kw = None

    for kw in matched_keywords:
        pos = low.find(str(kw).lower())
        if pos != -1:
            if first_pos is None or pos < first_pos:
                first_pos = pos
                matched_kw = kw

    if first_pos is None:
        snippet = original_text[:window * 2]
        return snippet.strip()

    start = max(0, first_pos - window)
    end = min(len(original_text), first_pos + window)
    snippet = original_text[start:end].strip()

    if start > 0:
        snippet = "..." + snippet
    if end < len(original_text):
        snippet = snippet + "..."

    return snippet


# =========================================================
# KEYWORDS
# =========================================================
# Imane Khelif
IMANE_STRONG = [
    "imane khelif",
    "khelif"
]

IMANE_SUPPORT = [
    "boxing",
    "boxer",
    "women boxing",
    "women's boxing",
    "female boxer",
    "olympic boxing",
    "algeria",
    "algerian",
    "eligibility",
    "gender",
    "sex",
    "intersex",
    "gender test",
    "male",
    "female"
]

# Opening ceremony / Christianity
CEREMONY_STRONG = [
    "opening ceremony",
    "last supper"
]

CEREMONY_SUPPORT = [
    "christianity",
    "christian",
    "jesus",
    "apostles",
    "religion",
    "religious",
    "faith",
    "biblical",
    "mock",
    "mockery",
    "blasphemy",
    "parody",
    "offend",
    "offensive"
]


# =========================================================
# CLASSIFIERS
# =========================================================
def classify_imane_article(text):
    strong_hits = count_keyword_hits(text, IMANE_STRONG)
    support_hits = count_keyword_hits(text, IMANE_SUPPORT)

    # πιο αυστηρό
    if len(strong_hits) >= 1 and len(support_hits) >= 1:
        label = "high_precision"
        confidence = "high"
    elif "imane khelif" in text:
        label = "high_precision"
        confidence = "high"
    elif len(strong_hits) >= 1:
        label = "review"
        confidence = "medium"
    elif len(support_hits) >= 4 and any(x in text for x in ["boxing", "boxer", "algeria", "algerian"]):
        label = "review"
        confidence = "low"
    else:
        label = "discard"
        confidence = "low"

    return {
        "label": label,
        "confidence": confidence,
        "strong_hits": strong_hits,
        "support_hits": support_hits,
        "all_hits": strong_hits + support_hits
    }


def classify_ceremony_article(text):
    strong_hits = count_keyword_hits(text, CEREMONY_STRONG)
    support_hits = count_keyword_hits(text, CEREMONY_SUPPORT)

    if len(strong_hits) >= 1 and len(support_hits) >= 2:
        label = "high_precision"
        confidence = "high"
    elif "opening ceremony" in text and any(x in text for x in ["christianity", "christian", "mockery", "blasphemy", "last supper"]):
        label = "high_precision"
        confidence = "high"
    elif len(strong_hits) >= 1 and len(support_hits) >= 1:
        label = "review"
        confidence = "medium"
    elif len(support_hits) >= 4:
        label = "review"
        confidence = "low"
    else:
        label = "discard"
        confidence = "low"

    return {
        "label": label,
        "confidence": confidence,
        "strong_hits": strong_hits,
        "support_hits": support_hits,
        "all_hits": strong_hits + support_hits
    }


# =========================================================
# LOAD DATA
# =========================================================
if not os.path.exists(INPUT_JSON):
    raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {INPUT_JSON}")

os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(INPUT_JSON, "r", encoding="utf-8") as f:
    articles = json.load(f)

if not isinstance(articles, list):
    raise ValueError("Το articles.json πρέπει να περιέχει λίστα από articles.")


# =========================================================
# PROCESS ARTICLES
# =========================================================
imane_rows = []
ceremony_rows = []

for idx, article in enumerate(articles):
    title = get_first_existing(article, ["title", "headline", "name"], "")
    text = get_first_existing(article, ["text", "content", "body", "article"], "")
    date = get_first_existing(article, ["publication-date", "date", "published_at", "published"], "")
    url = get_first_existing(article, ["url", "link"], "")
    source = get_first_existing(article, ["source", "site", "domain", "publisher"], "")

    combined_text = normalize_text(f"{title} {text}")

    # ---------- Imane ----------
    imane_result = classify_imane_article(combined_text)
    if imane_result["label"] != "discard":
        snippet = make_snippet(text if text else title, imane_result["all_hits"])
        imane_rows.append({
            "article_index": idx,
            "title": title,
            "publication_date": date,
            "source": source,
            "url": url,
            "label": imane_result["label"],
            "confidence": imane_result["confidence"],
            "strong_hits": ", ".join(imane_result["strong_hits"]),
            "support_hits": ", ".join(imane_result["support_hits"]),
            "all_hits_count": len(imane_result["all_hits"]),
            "snippet": snippet
        })

    # ---------- Ceremony ----------
    ceremony_result = classify_ceremony_article(combined_text)
    if ceremony_result["label"] != "discard":
        snippet = make_snippet(text if text else title, ceremony_result["all_hits"])
        ceremony_rows.append({
            "article_index": idx,
            "title": title,
            "publication_date": date,
            "source": source,
            "url": url,
            "label": ceremony_result["label"],
            "confidence": ceremony_result["confidence"],
            "strong_hits": ", ".join(ceremony_result["strong_hits"]),
            "support_hits": ", ".join(ceremony_result["support_hits"]),
            "all_hits_count": len(ceremony_result["all_hits"]),
            "snippet": snippet
        })


# =========================================================
# SAVE
# =========================================================
imane_df = pd.DataFrame(imane_rows)
ceremony_df = pd.DataFrame(ceremony_rows)

if not imane_df.empty:
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    label_rank = {"high_precision": 3, "review": 2}
    imane_df["_conf_rank"] = imane_df["confidence"].map(confidence_rank).fillna(0)
    imane_df["_label_rank"] = imane_df["label"].map(label_rank).fillna(0)
    imane_df = imane_df.sort_values(
        by=["_label_rank", "_conf_rank", "all_hits_count"],
        ascending=[False, False, False]
    ).drop(columns=["_conf_rank", "_label_rank"])

if not ceremony_df.empty:
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    label_rank = {"high_precision": 3, "review": 2}
    ceremony_df["_conf_rank"] = ceremony_df["confidence"].map(confidence_rank).fillna(0)
    ceremony_df["_label_rank"] = ceremony_df["label"].map(label_rank).fillna(0)
    ceremony_df = ceremony_df.sort_values(
        by=["_label_rank", "_conf_rank", "all_hits_count"],
        ascending=[False, False, False]
    ).drop(columns=["_conf_rank", "_label_rank"])

imane_df.to_csv(IMANE_OUTPUT, index=False, encoding="utf-8-sig")
ceremony_df.to_csv(CEREMONY_OUTPUT, index=False, encoding="utf-8-sig")


# =========================================================
# SUMMARY
# =========================================================
print("Ολοκληρώθηκε η ανάκτηση άρθρων.\n")

print("IMANE KHELIF")
if imane_df.empty:
    print("- Δεν βρέθηκαν σχετικά άρθρα.")
else:
    print(imane_df["label"].value_counts(dropna=False))
    print(f"- Αποθηκεύτηκε: {IMANE_OUTPUT}")

print("\nOPENING CEREMONY / CHRISTIANITY")
if ceremony_df.empty:
    print("- Δεν βρέθηκαν σχετικά άρθρα.")
else:
    print(ceremony_df["label"].value_counts(dropna=False))
    print(f"- Αποθηκεύτηκε: {CEREMONY_OUTPUT}")