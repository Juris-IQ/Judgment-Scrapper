#!/usr/bin/env python3
"""Create case-wise folders from the public AWS High Court tar archives.

Input layout:
    data/aws-public/<court>-pdfs/year=YYYY/bench=*/data.tar
    data/aws-public/<court>-metadata/year=YYYY/bench=*/metadata.tar.gz

Output layout:
    data/high-court/year=YYYY/court=<code>/bench=*/case=CNR/
        metadata.json
        pdfs/*.pdf

The original AWS archives are never changed.  No judgment/order filtering is
performed here; that decision can be made later during the upload stage.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from tqdm import tqdm

# Make direct execution from the repository work without PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.html_utils import parse_case_details_from_html


DEFAULT_COURT_CODE = "19~16"
DEFAULT_COURT_NAME = "Calcutta High Court"
CASE_ID_RE = re.compile(r"^[A-Z0-9]+$")


def safe_component(value: str, fallback: str = "unknown") -> str:
    value = re.sub(r"[^A-Za-z0-9._=-]+", "_", str(value).strip())
    return value or fallback


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("data/aws-public/calcutta-pdfs"),
        help="Downloaded AWS PDF archive directory",
    )
    parser.add_argument(
        "--metadata-dir",
        type=Path,
        default=Path("data/aws-public/calcutta-metadata"),
        help="Downloaded AWS metadata archive directory",
    )
    parser.add_argument(
        "--loose-metadata-dir",
        type=Path,
        help="Optional directory containing recovered loose metadata JSON files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/high-court"),
        help="Case-wise output directory",
    )
    parser.add_argument(
        "--court-code",
        default=DEFAULT_COURT_CODE,
        help="Court code in eCourts format, for example 19~16 or 7~26",
    )
    parser.add_argument(
        "--court-name",
        default=DEFAULT_COURT_NAME,
        help="Human-readable court name",
    )
    return parser.parse_args()


def load_json_from_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> dict[str, Any]:
    handle = archive.extractfile(member)
    if handle is None:
        raise ValueError(f"Could not read archive member {member.name}")
    value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object in {member.name}")
    return value


def archive_parts(index_root: Path, index_name: str) -> list[tuple[Path, Path, dict[str, Any]]]:
    """Return (index_path, archive_path, part_info) for every archive part."""
    parts: list[tuple[Path, Path, dict[str, Any]]] = []
    for index_path in sorted(index_root.rglob(index_name)):
        index_data = json.loads(index_path.read_text(encoding="utf-8"))
        for part in index_data.get("parts", []):
            archive_path = index_path.parent / part["name"]
            if not archive_path.exists():
                raise FileNotFoundError(f"Archive listed by index is missing: {archive_path}")
            parts.append((index_path, archive_path, part))
    return parts


def filename_from_member(member_name: str) -> str:
    return Path(member_name).name


def fallback_case_id(filename: str) -> str:
    stem = Path(filename).stem
    candidate = stem.split("_1_", 1)[0]
    return candidate if CASE_ID_RE.fullmatch(candidate) else stem


def details_from_metadata(metadata: dict[str, Any], court_name: str) -> dict[str, Any]:
    details = parse_case_details_from_html(metadata.get("raw_html", ""))
    return {
        "cnr": details.get("cnr") or "",
        "date_of_registration": details.get("date_of_registration") or "",
        "decision_date": details.get("decision_date"),
        "disposal_nature": details.get("disposal_nature") or "",
        "court": details.get("court") or metadata.get("court_name", court_name),
    }


def create_metadata_db(
    metadata_parts: list[tuple[Path, Path, dict[str, Any]]],
    loose_metadata_dir: Path | None = None,
) -> tuple[sqlite3.Connection, int, int]:
    temp_file = tempfile.NamedTemporaryFile(prefix="casewise_metadata_", suffix=".sqlite3", delete=False)
    temp_file.close()
    connection = sqlite3.connect(temp_file.name)
    connection.execute(
        "CREATE TABLE source_metadata (filename TEXT PRIMARY KEY, payload TEXT NOT NULL)"
    )

    inserted = 0
    duplicates = 0
    for _index_path, archive_path, _part in tqdm(
        metadata_parts, desc="Indexing metadata archives", unit="archive"
    ):
        mode = "r:gz" if archive_path.name.endswith(".gz") else "r:"
        with tarfile.open(archive_path, mode) as archive:
            batch: list[tuple[str, str]] = []
            for member in archive:
                if not member.isfile() or not member.name.lower().endswith(".json"):
                    continue
                filename = filename_from_member(member.name)
                payload = json.dumps(load_json_from_member(archive, member), ensure_ascii=False)
                batch.append((filename, payload))
                if len(batch) >= 1000:
                    before = connection.total_changes
                    connection.executemany(
                        "INSERT OR REPLACE INTO source_metadata(filename, payload) VALUES (?, ?)",
                        batch,
                    )
                    inserted += connection.total_changes - before
                    duplicates += len(batch) - (connection.total_changes - before)
                    batch.clear()
            if batch:
                before = connection.total_changes
                connection.executemany(
                    "INSERT OR REPLACE INTO source_metadata(filename, payload) VALUES (?, ?)",
                    batch,
                )
                inserted += connection.total_changes - before
                duplicates += len(batch) - (connection.total_changes - before)
        connection.commit()

    connection.execute("CREATE INDEX source_metadata_filename ON source_metadata(filename)")
    connection.commit()
    if loose_metadata_dir and loose_metadata_dir.exists():
        loose_rows: list[tuple[str, str]] = []
        for json_path in sorted(loose_metadata_dir.rglob("*.json")):
            if json_path.name == "recovery_report.json":
                continue
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            loose_rows.append((json_path.name, json.dumps(payload, ensure_ascii=False)))
            if len(loose_rows) >= 1000:
                connection.executemany(
                    "INSERT OR REPLACE INTO source_metadata(filename, payload) VALUES (?, ?)",
                    loose_rows,
                )
                inserted += len(loose_rows)
                loose_rows.clear()
        if loose_rows:
            connection.executemany(
                "INSERT OR REPLACE INTO source_metadata(filename, payload) VALUES (?, ?)",
                loose_rows,
            )
            inserted += len(loose_rows)
        connection.commit()
    return connection, inserted, duplicates


def parse_year_and_bench(index_path: Path) -> tuple[str, str]:
    year = next((part.split("=", 1)[1] for part in index_path.parts if part.startswith("year=")), "unknown")
    bench = next((part.split("=", 1)[1] for part in index_path.parts if part.startswith("bench=")), "unknown")
    return year, bench


def build_casewise(
    pdf_parts,
    metadata_db: sqlite3.Connection,
    output_dir: Path,
    court_code: str,
    court_name: str,
) -> dict[str, int]:
    total_pdfs = sum(len(part.get("files", [])) for _idx, _archive, part in pdf_parts)
    stats = {
        "pdfs_seen": 0,
        "pdfs_copied": 0,
        "pdfs_already_present": 0,
        "metadata_found": 0,
        "metadata_missing": 0,
        "case_folders": 0,
        "metadata_parse_failures": 0,
    }
    case_keys: set[tuple[str, str, str]] = set()
    missing_metadata_files: list[str] = []
    metadata_db.execute(
        "CREATE TABLE IF NOT EXISTS case_documents (case_key TEXT, year TEXT, bench TEXT, case_id TEXT, document_json TEXT)"
    )
    metadata_db.execute(
        "CREATE INDEX IF NOT EXISTS case_documents_lookup ON case_documents(year, bench, case_id)"
    )

    progress = tqdm(total=total_pdfs, desc="Building case-wise folders", unit="pdf")
    for index_path, archive_path, _part in pdf_parts:
        year, bench = parse_year_and_bench(index_path)
        with tarfile.open(archive_path, "r:") as archive:
            for member in archive:
                if not member.isfile() or not member.name.lower().endswith(".pdf"):
                    continue

                filename = filename_from_member(member.name)
                row = metadata_db.execute(
                    "SELECT payload FROM source_metadata WHERE filename = ?", (Path(filename).with_suffix(".json").name,)
                ).fetchone()
                metadata = json.loads(row[0]) if row else None
                if metadata is None:
                    stats["metadata_missing"] += 1
                    missing_metadata_files.append(filename)
                    metadata = {
                        "court_code": court_code,
                        "court_name": court_name,
                        "pdf_link": f"court/cnrorders/{bench}/orders/{filename}",
                        "downloaded": True,
                        "raw_html": "",
                    }
                    details = {
                        "cnr": fallback_case_id(filename),
                        "date_of_registration": "",
                        "decision_date": None,
                        "disposal_nature": "",
                        "court": court_name,
                    }
                else:
                    stats["metadata_found"] += 1
                    try:
                        details = details_from_metadata(metadata, court_name)
                    except Exception:
                        stats["metadata_parse_failures"] += 1
                        details = {
                            "cnr": fallback_case_id(filename),
                            "date_of_registration": "",
                            "decision_date": None,
                            "disposal_nature": "",
                            "court": court_name,
                        }

                case_id = details["cnr"] or fallback_case_id(filename)
                case_key = (year, bench, case_id)
                case_keys.add(case_key)
                case_dir = (
                    output_dir
                    / f"year={safe_component(year)}"
                    / f"court={safe_component(court_code.replace('~', '_'))}"
                    / f"bench={safe_component(bench)}"
                    / f"case={safe_component(case_id)}"
                )
                pdf_dir = case_dir / "pdfs"
                pdf_dir.mkdir(parents=True, exist_ok=True)
                destination = pdf_dir / filename
                if destination.exists():
                    stats["pdfs_already_present"] += 1
                else:
                    source = archive.extractfile(member)
                    if source is None:
                        raise ValueError(f"Could not extract {member.name}")
                    with destination.open("wb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)
                    stats["pdfs_copied"] += 1

                document = {
                    "filename": filename,
                    "path": f"pdfs/{filename}",
                    "pdf_link": metadata.get("pdf_link", ""),
                    "downloaded": bool(metadata.get("downloaded", True)),
                    "decision_date": details["decision_date"],
                    "date_of_registration": details["date_of_registration"],
                    "disposal_nature": details["disposal_nature"],
                    "metadata_found": row is not None,
                    "source_partition": f"year={year}/bench={bench}",
                    "raw_html": metadata.get("raw_html", ""),
                }
                metadata_db.execute(
                    "INSERT INTO case_documents VALUES (?, ?, ?, ?, ?)",
                    ("|".join(case_key), year, bench, case_id, json.dumps(document, ensure_ascii=False)),
                )
                stats["pdfs_seen"] += 1
                progress.update(1)
                progress.set_postfix(cases=len(case_keys), copied=stats["pdfs_copied"], missing_meta=stats["metadata_missing"])
    progress.close()
    metadata_db.commit()

    for year, bench, case_id in tqdm(sorted(case_keys), desc="Writing case metadata", unit="case"):
        case_dir = (
            output_dir
            / f"year={safe_component(year)}"
            / f"court={safe_component(court_code.replace('~', '_'))}"
            / f"bench={safe_component(bench)}"
            / f"case={safe_component(case_id)}"
        )
        rows = metadata_db.execute(
            "SELECT document_json FROM case_documents WHERE year=? AND bench=? AND case_id=? ORDER BY document_json",
            (year, bench, case_id),
        ).fetchall()
        documents = [json.loads(row[0]) for row in rows]
        aggregate = {
            "case_id": case_id,
            "court_code": court_code,
            "court_name": court_name,
            "bench": bench,
            "year": year,
            "document_count": len(documents),
            "documents": documents,
        }
        with (case_dir / "metadata.json").open("w", encoding="utf-8") as handle:
            json.dump(aggregate, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

    stats["case_folders"] = len(case_keys)
    if missing_metadata_files:
        (output_dir / "missing_metadata_pdfs.txt").write_text(
            "\n".join(sorted(set(missing_metadata_files))) + "\n", encoding="utf-8"
        )
    return stats


def main() -> None:
    args = parse_args()
    if not args.pdf_dir.exists() or not args.metadata_dir.exists():
        raise SystemExit("Both AWS PDF and metadata directories must exist")

    metadata_parts = archive_parts(args.metadata_dir, "metadata.index.json")
    pdf_parts = archive_parts(args.pdf_dir, "data.index.json")
    if not pdf_parts:
        raise SystemExit("No PDF archives found")

    metadata_db, metadata_count, duplicate_count = create_metadata_db(
        metadata_parts,
        args.loose_metadata_dir,
    )
    try:
        stats = build_casewise(
            pdf_parts,
            metadata_db,
            args.output_dir,
            args.court_code,
            args.court_name,
        )
        report = {
            "court_code": args.court_code,
            "court_name": args.court_name,
            "metadata_records_indexed": metadata_count,
            "duplicate_metadata_filenames": duplicate_count,
            **stats,
        }
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "build_report.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, indent=2))
    finally:
        db_path = Path(metadata_db.execute("PRAGMA database_list").fetchone()[2])
        metadata_db.close()
        db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
