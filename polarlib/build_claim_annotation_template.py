import os
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
RESULTS_DIR = "results"

IMANE_INPUT = os.path.join(RESULTS_DIR, "articles_imane_khelif.csv")
CEREMONY_INPUT = os.path.join(RESULTS_DIR, "articles_opening_ceremony_christianity.csv")

OUTPUT_CSV = os.path.join(RESULTS_DIR, "claim_annotation_template.csv")

# πόσα άρθρα να κρατήσει ανά cluster
TARGET_PER_CLUSTER = 25

# αν δεν φτάνουν τα high_precision, πάρε και review
ALLOW_REVIEW_FILL = True


# =========================================================
# HELPERS
# =========================================================
def load_articles(path, cluster_name):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Δεν βρέθηκε το αρχείο: {path}")

    df = pd.read_csv(path)

    # expected columns from previous retrieval script
    # article_index, title, publication_date, source, url, label, confidence, strong_hits, support_hits, all_hits_count, snippet
    for col in [
        "article_index", "title", "publication_date", "source", "url",
        "label", "confidence", "strong_hits", "support_hits", "all_hits_count", "snippet"
    ]:
        if col not in df.columns:
            df[col] = ""

    df = df.copy()
    df["cluster"] = cluster_name

    # sorting: high_precision first, then review
    label_rank = {"high_precision": 2, "review": 1}
    confidence_rank = {"high": 3, "medium": 2, "low": 1}

    df["_label_rank"] = df["label"].map(label_rank).fillna(0)
    df["_confidence_rank"] = df["confidence"].map(confidence_rank).fillna(0)
    df["_hits_rank"] = pd.to_numeric(df["all_hits_count"], errors="coerce").fillna(0)

    df = df.sort_values(
        by=["_label_rank", "_confidence_rank", "_hits_rank"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    return df


def select_sample(df, target_n, allow_review_fill=True):
    high_df = df[df["label"] == "high_precision"].copy()
    review_df = df[df["label"] == "review"].copy()

    if len(high_df) >= target_n:
        return high_df.head(target_n).copy()

    selected = high_df.copy()

    if allow_review_fill and len(selected) < target_n:
        needed = target_n - len(selected)
        selected = pd.concat([selected, review_df.head(needed)], ignore_index=True)

    return selected.reset_index(drop=True)


# =========================================================
# LOAD AND SAMPLE
# =========================================================
imane_df = load_articles(IMANE_INPUT, "Cluster A - Khelif / gender / eligibility")
ceremony_df = load_articles(CEREMONY_INPUT, "Cluster B - Opening Ceremony / Christianity")

imane_sample = select_sample(imane_df, TARGET_PER_CLUSTER, allow_review_fill=ALLOW_REVIEW_FILL)
ceremony_sample = select_sample(ceremony_df, TARGET_PER_CLUSTER, allow_review_fill=ALLOW_REVIEW_FILL)

sample_df = pd.concat([imane_sample, ceremony_sample], ignore_index=True)


# =========================================================
# BUILD ANNOTATION TEMPLATE
# =========================================================
output_rows = []

for _, row in sample_df.iterrows():
    output_rows.append({
        # source article info
        "cluster": row.get("cluster", ""),
        "article_index": row.get("article_index", ""),
        "title": row.get("title", ""),
        "publication_date": row.get("publication_date", ""),
        "source": row.get("source", ""),
        "url": row.get("url", ""),
        "retrieval_label": row.get("label", ""),
        "retrieval_confidence": row.get("confidence", ""),
        "strong_hits": row.get("strong_hits", ""),
        "support_hits": row.get("support_hits", ""),
        "all_hits_count": row.get("all_hits_count", ""),
        "snippet": row.get("snippet", ""),

        # manual annotation fields
        "claim_1_text": "",
        "claim_1_category": "",   # Verified fact / Misleading framing / False
        "claim_1_why": "",

        "claim_2_text": "",
        "claim_2_category": "",
        "claim_2_why": "",

        "claim_3_text": "",
        "claim_3_category": "",
        "claim_3_why": "",

        "dominant_claim_type_in_article": "",
        "article_notes": "",
        "keep_for_case_study": "",
    })

annotation_df = pd.DataFrame(output_rows)

# sort by cluster then relevance
annotation_df = annotation_df.sort_values(
    by=["cluster", "retrieval_label", "retrieval_confidence", "all_hits_count"],
    ascending=[True, False, False, False]
).reset_index(drop=True)

# drop helper cols from load sorting if present
for col in ["_label_rank", "_confidence_rank", "_hits_rank"]:
    if col in annotation_df.columns:
        annotation_df = annotation_df.drop(columns=[col])

# save
annotation_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("Ολοκληρώθηκε η δημιουργία του claim annotation template.")
print(f"Αποθηκεύτηκε εδώ: {OUTPUT_CSV}")
print(f"Σύνολο άρθρων στο template: {len(annotation_df)}")

print("\nΚατανομή ανά cluster:")
print(annotation_df["cluster"].value_counts(dropna=False))

print("\nΚατανομή retrieval_label:")
print(annotation_df["retrieval_label"].value_counts(dropna=False))