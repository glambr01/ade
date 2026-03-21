import os
import json
import pickle
import csv
from typing import Any, Dict, List, Optional, Tuple


# =========================================================
# ΡΥΘΜΙΣΕΙΣ
# =========================================================
BASE_PATH = "polar_articles"

TOPICS_FILE = os.path.join(BASE_PATH, "topics.json")
FELLOWSHIPS_FILE = os.path.join(BASE_PATH, "polarization", "fellowships.json")
DIPOLES_FILE = os.path.join(BASE_PATH, "polarization", "dipoles.pckl")

INPUT_CSV = "polar_articles/analysis_results/analyzer_results.csv"

TOP_N_TOPIC_PHRASES = 10
TOP_N_FELLOWSHIP_MEMBERS = 20
TOP_N_SIDE_ENTITIES = 20

INCLUDE_INTERPRETATION = True

OUTPUT_JSON = "polar_articles/analysis_results/matched_results.json"
OUTPUT_CSV = "polar_articles/analysis_results/matched_results.csv"
OUTPUT_TXT = "polar_articles/analysis_results/matched_report.txt"


# =========================================================
# ΦΟΡΤΩΣΗ ΔΕΔΟΜΕΝΩΝ
# =========================================================
def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_pickle(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


def load_resources() -> Tuple[Dict[str, Any], List[Any], List[Any]]:
    topics = load_json(TOPICS_FILE)
    fellowships_data = load_json(FELLOWSHIPS_FILE)
    dipoles = load_pickle(DIPOLES_FILE)

    fellowships = fellowships_data["fellowships"]
    return topics, fellowships, dipoles


# =========================================================
# CSV INPUT
# =========================================================
def read_input_csv(path: str) -> List[Dict[str, Any]]:
    rows = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"label", "pi", "obs", "dipole", "topic"}
        found_columns = set(reader.fieldnames or [])

        missing = required_columns - found_columns
        if missing:
            raise ValueError(
                f"Το CSV δεν έχει τις απαιτούμενες στήλες: {sorted(missing)}. "
                f"Βρέθηκαν: {sorted(found_columns)}"
            )

        for i, row in enumerate(reader, start=2): 
            try:
                parsed = {
                    "label": row["label"].strip(),
                    "pi": float(row["pi"]),
                    "obs": int(float(row["obs"])),
                    "dipole": row["dipole"].strip(),
                    "topic": row["topic"].strip(),
                }
                rows.append(parsed)
            except Exception as e:
                raise ValueError(f"Σφάλμα στη γραμμή {i} του CSV: {row} -> {e}")

    return rows


# =========================================================
# ΒΟΗΘΗΤΙΚΕΣ ΣΥΝΑΡΤΗΣΕΙΣ
# =========================================================
def parse_dipole_id(dipole_id: str) -> Tuple[int, int]:
    if not dipole_id.startswith("D"):
        raise ValueError(f"Invalid dipole id: {dipole_id}")

    parts = dipole_id[1:].split("_")
    if len(parts) != 2:
        raise ValueError(f"Invalid dipole id format: {dipole_id}")

    return int(parts[0]), int(parts[1])


def get_topic_phrases(topic_id: str, topics: Dict[str, Any], top_n: int = 10) -> List[str]:
    if topic_id not in topics:
        return []

    topic_data = topics[topic_id]

    if isinstance(topic_data, dict):
        phrases = topic_data.get("noun_phrases", [])
        if isinstance(phrases, list):
            return phrases[:top_n]

    return []


def get_dipole_info(dipole_id: str, dipoles: List[Any]) -> Optional[Dict[str, Any]]:
    target = parse_dipole_id(dipole_id)

    for item in dipoles:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue

        pair, meta = item

        if pair == target:
            return {
                "pair": pair,
                "pos": meta.get("pos", 0),
                "neg": meta.get("neg", 0),
                "simap_1": meta.get("simap_1", []),
                "simap_2": meta.get("simap_2", []),
                "positive_ratio": meta.get("positive_ratio", 0),
                "negative_ratio": meta.get("negative_ratio", 0),
            }

    return None


def get_fellowship_members(fellowship_id: int, fellowships: List[Any], top_n: int = 20) -> List[Any]:
    if 0 <= fellowship_id < len(fellowships):
        members = fellowships[fellowship_id]
        if isinstance(members, list):
            return members[:top_n]
        return members
    return []


def polarization_level(pi: float) -> str:
    if pi == 0:
        return "no polarization"
    elif pi <= 0.33:
        return "low polarization"
    elif pi <= 0.66:
        return "moderate polarization"
    else:
        return "high polarization"


def build_interpretation_from_matched(result: Dict[str, Any]) -> str:
    level = polarization_level(result["pi"])

    topic_preview = ", ".join(result["topic_phrases"][:3]) if result["topic_phrases"] else result["topic_id"]
    side1_preview = ", ".join(map(str, result["side_1_entities"][:3])) if result["side_1_entities"] else "N/A"
    side2_preview = ", ".join(map(str, result["side_2_entities"][:3])) if result["side_2_entities"] else "N/A"

    return (
        f"The topic {result['topic_id']} is represented by phrases such as: {topic_preview}. "
        f"The dipole {result['dipole_id']} corresponds to fellowships "
        f"{result['fellowship_pair'][0]} and {result['fellowship_pair'][1]}. "
        f"Representative entities for the first side include: {side1_preview}. "
        f"Representative entities for the second side include: {side2_preview}. "
        f"The observed polarization level is {level} (pi={result['pi']}, obs={result['obs']})."
    )


# =========================================================
# MATCHING
# =========================================================
def build_matched_result(
    row: Dict[str, Any],
    topics: Dict[str, Any],
    dipoles: List[Any],
    fellowships: List[Any],
) -> Dict[str, Any]:
    topic_id = row["topic"]
    dipole_id = row["dipole"]
    pi = row["pi"]
    obs = row["obs"]
    label = row.get("label", "")

    topic_phrases = get_topic_phrases(topic_id, topics, TOP_N_TOPIC_PHRASES)
    dipole_info = get_dipole_info(dipole_id, dipoles)

    result: Dict[str, Any] = {
        "label": label,
        "topic_id": topic_id,
        "topic_phrases": topic_phrases,
        "dipole_id": dipole_id,
        "pi": pi,
        "obs": obs,
    }

    if dipole_info is None:
        result.update({
            "fellowship_pair": [],
            "fellowship_1_members": [],
            "fellowship_2_members": [],
            "side_1_entities": [],
            "side_2_entities": [],
            "pos": None,
            "neg": None,
            "positive_ratio": None,
            "negative_ratio": None,
            "warning": f"Dipole {dipole_id} was not found in dipoles.pckl",
        })
    else:
        f1, f2 = dipole_info["pair"]

        result.update({
            "fellowship_pair": [f1, f2],
            "fellowship_1_members": get_fellowship_members(f1, fellowships, TOP_N_FELLOWSHIP_MEMBERS),
            "fellowship_2_members": get_fellowship_members(f2, fellowships, TOP_N_FELLOWSHIP_MEMBERS),
            "side_1_entities": dipole_info["simap_1"][:TOP_N_SIDE_ENTITIES],
            "side_2_entities": dipole_info["simap_2"][:TOP_N_SIDE_ENTITIES],
            "pos": dipole_info["pos"],
            "neg": dipole_info["neg"],
            "positive_ratio": dipole_info["positive_ratio"],
            "negative_ratio": dipole_info["negative_ratio"],
        })

    if INCLUDE_INTERPRETATION:
        result["interpretation"] = build_interpretation_from_matched(result)

    return result


def process_rows(
    rows: List[Dict[str, Any]],
    topics: Dict[str, Any],
    dipoles: List[Any],
    fellowships: List[Any],
) -> List[Dict[str, Any]]:
    return [build_matched_result(row, topics, dipoles, fellowships) for row in rows]


# =========================================================
# OUTPUT
# =========================================================
def save_json(results: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def save_csv(results: List[Dict[str, Any]], path: str) -> None:
    fieldnames = [
        "label",
        "topic_id",
        "dipole_id",
        "pi",
        "obs",
        "fellowship_pair",
        "topic_phrases",
        "side_1_entities",
        "side_2_entities",
        "pos",
        "neg",
        "positive_ratio",
        "negative_ratio",
        "interpretation",
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in results:
            writer.writerow({
                "label": r.get("label"),
                "topic_id": r.get("topic_id"),
                "dipole_id": r.get("dipole_id"),
                "pi": r.get("pi"),
                "obs": r.get("obs"),
                "fellowship_pair": json.dumps(r.get("fellowship_pair", []), ensure_ascii=False),
                "topic_phrases": json.dumps(r.get("topic_phrases", []), ensure_ascii=False),
                "side_1_entities": json.dumps(r.get("side_1_entities", []), ensure_ascii=False),
                "side_2_entities": json.dumps(r.get("side_2_entities", []), ensure_ascii=False),
                "pos": r.get("pos"),
                "neg": r.get("neg"),
                "positive_ratio": r.get("positive_ratio"),
                "negative_ratio": r.get("negative_ratio"),
                "interpretation": r.get("interpretation", ""),
            })


def save_txt_report(results: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in results:
            f.write("=" * 80 + "\n")
            f.write(f"Label: {r.get('label', '')}\n")
            f.write(f"Topic ID: {r.get('topic_id', '')}\n")
            f.write(f"Dipole ID: {r.get('dipole_id', '')}\n")
            f.write(f"PI: {r.get('pi', '')}\n")
            f.write(f"OBS: {r.get('obs', '')}\n")
            f.write(f"Fellowship Pair: {r.get('fellowship_pair', [])}\n\n")

            f.write("Topic Phrases:\n")
            for item in r.get("topic_phrases", []):
                f.write(f"  - {item}\n")
            f.write("\n")

            f.write("Fellowship 1 Members:\n")
            for item in r.get("fellowship_1_members", []):
                f.write(f"  - {item}\n")
            f.write("\n")

            f.write("Fellowship 2 Members:\n")
            for item in r.get("fellowship_2_members", []):
                f.write(f"  - {item}\n")
            f.write("\n")

            f.write("Side 1 Entities:\n")
            for item in r.get("side_1_entities", []):
                f.write(f"  - {item}\n")
            f.write("\n")

            f.write("Side 2 Entities:\n")
            for item in r.get("side_2_entities", []):
                f.write(f"  - {item}\n")
            f.write("\n")

            f.write(f"Positive edges: {r.get('pos')}\n")
            f.write(f"Negative edges: {r.get('neg')}\n")
            f.write(f"Positive ratio: {r.get('positive_ratio')}\n")
            f.write(f"Negative ratio: {r.get('negative_ratio')}\n\n")

            if "interpretation" in r:
                f.write("Interpretation:\n")
                f.write(r["interpretation"] + "\n\n")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    if not os.path.exists(BASE_PATH):
        raise FileNotFoundError(
            f"Δεν βρέθηκε ο φάκελος '{BASE_PATH}'. "
            f"Βεβαιώσου ότι έχεις κάνει unzip το polar_articles.zip."
        )

    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(
            f"Δεν βρέθηκε το input CSV '{INPUT_CSV}'."
        )

    topics, fellowships, dipoles = load_resources()
    input_rows = read_input_csv(INPUT_CSV)
    results = process_rows(input_rows, topics, dipoles, fellowships)

    save_json(results, OUTPUT_JSON)
    save_csv(results, OUTPUT_CSV)
    save_txt_report(results, OUTPUT_TXT)

    print("Done. Files created:")
    print(f"  - {OUTPUT_JSON}")
    print(f"  - {OUTPUT_CSV}")
    print(f"  - {OUTPUT_TXT}")


if __name__ == "__main__":
    main()