import json
from pathlib import Path
from datetime import date

from polarlib.polar.news_corpus_collector import NewsCorpusCollector

INPUT_JSON = "articles.json"
OUTPUT_DIR = "polar_articles"


def get_article_day(article):
    pub_date = (article.get("publication-date") or "").strip()

    if pub_date:
        return pub_date[:10].replace("-", "")
    return "unknown_date"


def seed_articles_from_json(input_json, output_dir):
    with open(input_json, "r", encoding="utf-8") as f:
        articles = json.load(f)

    kept = 0

    for article in articles:
        uid = article.get("uid")
        text = article.get("text", "")

        if not uid or not text.strip():
            continue

        day_folder = get_article_day(article)
        out_dir = Path(output_dir) / "articles" / day_folder
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{uid}.json"

        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(article, fw, ensure_ascii=False)

        kept += 1

    return kept


def preprocess_seeded_articles(output_dir):
    corpus_collector = NewsCorpusCollector(
        output_dir=output_dir,
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 2),
        keywords=[]
    )

    articles_root = Path(output_dir) / "articles"

    if not articles_root.exists():
        raise FileNotFoundError(f"Articles directory not found: {articles_root}")

    processed = 0

    for day_dir in sorted(articles_root.iterdir()):
        if not day_dir.is_dir():
            continue

        for article_file in sorted(day_dir.glob("*.json")):
            ok = corpus_collector.pre_process_article(str(article_file))
            if ok:
                processed += 1

    return processed


if __name__ == "__main__":
    n_seeded = seed_articles_from_json(INPUT_JSON, OUTPUT_DIR)
    n_processed = preprocess_seeded_articles(OUTPUT_DIR)

    print("Done.")
    print("Seeded articles:", n_seeded)
    print("Preprocessed articles:", n_processed)
    print("Articles dir:", f"{OUTPUT_DIR}/articles")
    print("Preprocessed dir:", f"{OUTPUT_DIR}/pre_processed")