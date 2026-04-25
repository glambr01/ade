import json
import pandas as pd
import re
from pathlib import Path

# ====== FILES ======
DATASET_JSON = "dataset.json"
KHELIF_CSV = "results/articles_imane_khelif.csv"
CEREMONY_CSV = "results/articles_opening_ceremony_christianity.csv"

OUT_KHELIF = "results/articles_imane_khelif_with_fulltext.csv"
OUT_CEREMONY = "results/articles_opening_ceremony_with_fulltext.csv"
OUT_MATCH_LOG = "match_log.csv"


def normalize_text(s):
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[“”\"'`]", "", s)
    return s


def normalize_url(u):
    if not u:
        return ""
    u = str(u).strip().lower()
    u = u.replace("http://", "").replace("https://", "")
    u = u.rstrip("/")
    return u


def normalize_title(t):
    t = normalize_text(t)
    # remove common punctuation for easier matching
    t = re.sub(r"[^a-z0-9\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ====== LOAD DATASET ======
with open(DATASET_JSON, "r", encoding="utf-8") as f:
    dataset = json.load(f)

dataset_df = pd.DataFrame(dataset)

# expected useful columns in dataset.json:
# title, url, text or full_text, source, publication-date / publication_date
dataset_df["url_norm"] = dataset_df.get("url", "").apply(normalize_url)
dataset_df["title_norm"] = dataset_df.get("title", "").apply(normalize_title)

# choose whichever text column exists
text_col = None
for candidate in ["full_text", "text", "article_text", "content"]:
    if candidate in dataset_df.columns:
        text_col = candidate
        break

if text_col is None:
    raise ValueError("No text column found in dataset.json. Expected one of: full_text, text, article_text, content")


def enrich_csv(csv_path, output_path):
    df = pd.read_csv(csv_path)

    if "url" not in df.columns or "title" not in df.columns:
        raise ValueError(f"{csv_path} must contain at least 'url' and 'title' columns")

    df["url_norm"] = df["url"].apply(normalize_url)
    df["title_norm"] = df["title"].apply(normalize_title)

    matched_rows = []
    logs = []

    for _, row in df.iterrows():
        match_type = None
        matched = None

        # 1) exact URL match
        if row["url_norm"]:
            candidates = dataset_df[dataset_df["url_norm"] == row["url_norm"]]
            if len(candidates) == 1:
                matched = candidates.iloc[0]
                match_type = "url_exact"
            elif len(candidates) > 1:
                matched = candidates.iloc[0]
                match_type = "url_multiple_first"

        # 2) exact normalized title match
        if matched is None and row["title_norm"]:
            candidates = dataset_df[dataset_df["title_norm"] == row["title_norm"]]
            if len(candidates) == 1:
                matched = candidates.iloc[0]
                match_type = "title_exact"
            elif len(candidates) > 1:
                matched = candidates.iloc[0]
                match_type = "title_multiple_first"

        # prepare output row
        out_row = row.to_dict()

        if matched is not None:
            out_row["matched_title"] = matched.get("title", "")
            out_row["matched_url"] = matched.get("url", "")
            out_row["matched_source"] = matched.get("source", "")
            out_row["matched_publication_date"] = matched.get("publication-date", matched.get("publication_date", ""))
            out_row["full_text"] = matched.get(text_col, "")
            out_row["match_type"] = match_type
            out_row["matched_ok"] = "yes"
        else:
            out_row["matched_title"] = ""
            out_row["matched_url"] = ""
            out_row["matched_source"] = ""
            out_row["matched_publication_date"] = ""
            out_row["full_text"] = ""
            out_row["match_type"] = "no_match"
            out_row["matched_ok"] = "no"

        matched_rows.append(out_row)

        logs.append({
            "input_csv": csv_path,
            "title": row.get("title", ""),
            "url": row.get("url", ""),
            "matched_ok": out_row["matched_ok"],
            "match_type": out_row["match_type"],
            "matched_title": out_row["matched_title"],
            "matched_url": out_row["matched_url"],
        })

    out_df = pd.DataFrame(matched_rows)
    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    return pd.DataFrame(logs)


log1 = enrich_csv(KHELIF_CSV, OUT_KHELIF)
log2 = enrich_csv(CEREMONY_CSV, OUT_CEREMONY)

match_log = pd.concat([log1, log2], ignore_index=True)
match_log.to_csv(OUT_MATCH_LOG, index=False, encoding="utf-8-sig")

print("Done.")
print(f"Created: {OUT_KHELIF}")
print(f"Created: {OUT_CEREMONY}")
print(f"Created: {OUT_MATCH_LOG}")
print()
print(match_log["matched_ok"].value_counts(dropna=False))