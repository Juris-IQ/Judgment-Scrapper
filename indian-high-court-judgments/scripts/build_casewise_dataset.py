#!/usr/bin/env python3
"""Build a case-wise local layout from the scraper's PDF/JSON sidecars.

The downloader stores one JSON sidecar beside each PDF.  This script keeps the
original layout untouched and creates one directory per CNR instead:

    data/high-court/year=2026/court=19_16/bench=.../case=WBCHCA.../
        metadata.json
        pdfs/
            WBCHCA...pdf

Only PDFs are used as the input list, so result rows which were skipped because
the PDF already exists in S3 do not create empty case folders.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# Allow the script to be run directly from the repository root or from any
# working directory without requiring PYTHONPATH to be set.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.utils.html_utils import parse_case_details_from_html


CASE_ID_RE = re.compile(r"^[A-Z0-9]+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/court/cnrorders"),
        help="Downloader output directory (default: data/court/cnrorders)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/high-court"),
        help="Case-wise output directory (default: data/high-court)",
    )
    return parser.parse_args()


def safe_component(value: str, fallback: str = "unknown") -> str:
    value = re.sub(r"[^A-Za-z0-9._=-]+", "_", value.strip())
    return value or fallback


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object in {path}")
    return value


def get_case_details(metadata: dict[str, Any]) -> dict[str, Any]:
    raw_html = metadata.get("raw_html", "")
    details = parse_case_details_from_html(raw_html)
    return {
        "cnr": details.get("cnr") or "",
        "date_of_registration": details.get("date_of_registration") or "",
        "decision_date": details.get("decision_date"),
        "disposal_nature": details.get("disposal_nature") or "",
        "court": details.get("court") or metadata.get("court_name", ""),
    }


def fallback_case_id(pdf_path: Path) -> str:
    # Downloader filenames normally begin with the CNR and then contain the
    # document sequence/date, for example CNR_1_2026-01-05.pdf.
    candidate = pdf_path.stem.split("_1_", 1)[0]
    return candidate if CASE_ID_RE.fullmatch(candidate) else pdf_path.stem


def decision_year(documents: list[dict[str, Any]]) -> str:
    years: list[str] = []
    for document in documents:
        value = document.get("decision_date") or ""
        match = re.search(r"(\d{4})$", value)
        if match:
            years.append(match.group(1))
    return min(years) if years else "unknown"


def build_casewise(input_dir: Path, output_dir: Path) -> tuple[int, int, int]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    missing_metadata = 0

    pdf_paths = sorted(input_dir.rglob("*.pdf"))
    for pdf_path in pdf_paths:
        metadata_path = pdf_path.with_suffix(".json")
        if not metadata_path.exists():
            print(f"WARNING: missing sidecar, skipping {pdf_path}")
            missing_metadata += 1
            continue

        metadata = read_json(metadata_path)
        details = get_case_details(metadata)
        case_id = details["cnr"] or fallback_case_id(pdf_path)

        relative_parts = pdf_path.relative_to(input_dir).parts
        bench = relative_parts[0] if relative_parts else "unknown"
        court_code = str(metadata.get("court_code", "unknown")).replace("~", "_")
        court_name = metadata.get("court_name", "")

        grouped[(court_code, bench, case_id, court_name)].append(
            {
                "source_pdf": pdf_path,
                "source_metadata": metadata_path,
                "details": details,
                "metadata": metadata,
            }
        )

    case_count = 0
    document_count = 0
    for (court_code, bench, case_id, court_name), records in grouped.items():
        records.sort(key=lambda record: record["source_pdf"].name)
        documents: list[dict[str, Any]] = []

        for record in records:
            source_pdf: Path = record["source_pdf"]
            details = record["details"]
            documents.append(
                {
                    "filename": source_pdf.name,
                    "path": f"pdfs/{source_pdf.name}",
                    "pdf_link": record["metadata"].get("pdf_link", ""),
                    "downloaded": bool(record["metadata"].get("downloaded", False)),
                    "decision_date": details["decision_date"],
                    "date_of_registration": details["date_of_registration"],
                    "disposal_nature": details["disposal_nature"],
                    "source_path": str(source_pdf),
                    # Preserve the portal row without creating another JSON
                    # file per document.
                    "raw_html": record["metadata"].get("raw_html", ""),
                }
            )

        year = decision_year(documents)
        case_dir = (
            output_dir
            / f"year={safe_component(year)}"
            / f"court={safe_component(court_code)}"
            / f"bench={safe_component(bench)}"
            / f"case={safe_component(case_id)}"
        )
        pdf_dir = case_dir / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)

        for record in records:
            shutil.copy2(record["source_pdf"], pdf_dir / record["source_pdf"].name)

        aggregate = {
            "case_id": case_id,
            "court_code": court_code.replace("_", "~", 1),
            "court_name": court_name,
            "bench": bench,
            "case_year": year,
            "document_count": len(documents),
            "documents": documents,
        }
        with (case_dir / "metadata.json").open("w", encoding="utf-8") as handle:
            json.dump(aggregate, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

        case_count += 1
        document_count += len(documents)

    return case_count, document_count, missing_metadata


def main() -> None:
    args = parse_args()
    if not args.input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")

    case_count, document_count, missing_metadata = build_casewise(
        args.input_dir, args.output_dir
    )
    print(f"Created/updated {case_count} case folder(s)")
    print(f"Copied {document_count} PDF(s)")
    if missing_metadata:
        print(f"Skipped {missing_metadata} PDF(s) without matching metadata")


if __name__ == "__main__":
    main()
