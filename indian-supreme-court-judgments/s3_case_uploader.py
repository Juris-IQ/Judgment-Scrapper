#!/usr/bin/env python3
"""Upload Supreme Court judgments to a case-centric S3 layout.

Reads the local tar archive layout produced by download.py:

    local_sc_judgments_data/<year>/
      metadata.tar
      english.tar
      regional.tar

Uploads to:

    supreme-court/year=<YEAR>/case=<CASE_ID>/metadata.json
    supreme-court/year=<YEAR>/case=<CASE_ID>/pdfs/<LANG>.pdf

Credentials are resolved by boto3's default provider chain:
EC2 IAM role, environment, shared AWS config, or AWS CLI credentials.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError


DEFAULT_DATA_DIR = Path("local_sc_judgments_data")
DEFAULT_PREFIX = "supreme-court"
DEFAULT_PROGRESS_SUFFIX = "upload_progress.json"
DEFAULT_REPORT_SUFFIX = "upload_report.json"
DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024
DEFAULT_MULTIPART_CHUNKSIZE = 64 * 1024 * 1024

ARCHIVE_FILES = {
    "metadata": "metadata.tar",
    "english": "english.tar",
    "regional": "regional.tar",
}

Status = Literal["uploaded", "failed", "skipped"]
SourceType = Literal["tar", "file"]


@dataclass(frozen=True)
class UploadItem:
    year: str
    source_type: SourceType
    archive_type: str
    source_path: str
    member_name: str
    s3_key: str
    content_type: str
    size: int


@dataclass
class UploadReport:
    uploaded: int = 0
    failed: int = 0
    skipped: int = 0
    by_year: dict[str, dict[str, int]] = field(default_factory=dict)

    def record(self, year: str, status: Status) -> None:
        if status == "uploaded":
            self.uploaded += 1
        elif status == "skipped":
            self.skipped += 1
        else:
            self.failed += 1

        year_counts = self.by_year.setdefault(
            year,
            {"uploaded": 0, "failed": 0, "skipped": 0},
        )
        year_counts[status] += 1


class ProgressStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {"items": {}}
        with self.path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if "items" not in data or not isinstance(data["items"], dict):
            data["items"] = {}
        return data

    def already_done(self, s3_key: str) -> bool:
        item = self.data["items"].get(s3_key)
        return bool(item and item.get("status") in {"uploaded", "skipped"})

    def record(self, item: UploadItem, status: Status, error: str | None = None) -> None:
        payload = {
            "status": status,
            "year": item.year,
            "source_type": item.source_type,
            "archive_type": item.archive_type,
            "source_path": item.source_path,
            "member_name": item.member_name,
            "s3_key": item.s3_key,
            "size": item.size,
            "updated_at": utc_now(),
        }
        if error:
            payload["error"] = error

        with self.lock:
            self.data["items"][item.s3_key] = payload
            self._write_locked()

    def _write_locked(self) -> None:
        self.data["updated_at"] = utc_now()
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, sort_keys=True)
        tmp_path.replace(self.path)


class S3CaseUploader:
    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        prefix: str,
        data_dir: Path,
        progress_file: Path,
        report_file: Path,
        workers: int,
        retries: int,
        dry_run: bool,
    ):
        self.bucket = bucket
        self.region = region
        self.prefix = prefix.strip("/")
        self.data_dir = data_dir
        self.progress = ProgressStore(progress_file)
        self.progress_file = progress_file
        self.report_file = report_file
        self.workers = workers
        self.retries = retries
        self.dry_run = dry_run
        self.s3 = boto3.client("s3", region_name=region)
        self.transfer_config = TransferConfig(
            multipart_threshold=DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunksize=DEFAULT_MULTIPART_CHUNKSIZE,
            max_concurrency=4,
            use_threads=True,
        )
        self.report = UploadReport()
        self.report_lock = threading.Lock()

    def run(self, years: list[str] | None = None) -> UploadReport:
        items = self.discover_items(years)
        logging.info("Discovered %s upload item(s)", len(items))

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(self.process_item, item) for item in items]
            for future in as_completed(futures):
                item, status = future.result()
                with self.report_lock:
                    self.report.record(item.year, status)

        self.write_report()
        return self.report

    def discover_items(self, years: list[str] | None) -> list[UploadItem]:
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

        wanted_years = set(years) if years else None
        items: list[UploadItem] = []

        for year_dir in sorted(self.data_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            if wanted_years and year_dir.name not in wanted_years:
                continue

            for archive_type, archive_name in ARCHIVE_FILES.items():
                archive_path = year_dir / archive_name
                if not archive_path.exists():
                    continue
                items.extend(self._items_from_archive(year_dir.name, archive_type, archive_path))

            items.extend(self._items_from_expanded_year_dir(year_dir.name, year_dir))

        return dedupe_items(items)

    def _items_from_archive(
        self, year: str, archive_type: str, archive_path: Path
    ) -> list[UploadItem]:
        import tarfile

        items: list[UploadItem] = []
        with tarfile.open(archive_path, "r") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                member_name = Path(member.name).name
                if archive_type == "metadata":
                    if not member_name.endswith(".json"):
                        continue
                    case_id = Path(member_name).stem
                    s3_key = self._metadata_key(year, case_id)
                    content_type = "application/json"
                else:
                    if not member_name.endswith(".pdf"):
                        continue
                    case_id, lang = parse_pdf_member(member_name)
                    s3_key = self._pdf_key(year, case_id, lang)
                    content_type = "application/pdf"

                items.append(
                    UploadItem(
                        year=year,
                        source_type="tar",
                        archive_type=archive_type,
                        source_path=str(archive_path),
                        member_name=member.name,
                        s3_key=s3_key,
                        content_type=content_type,
                        size=member.size,
                    )
                )
        return items

    def _items_from_expanded_year_dir(self, year: str, year_dir: Path) -> list[UploadItem]:
        items: list[UploadItem] = []

        for path in sorted(year_dir.iterdir()):
            if path.is_file():
                item = self._item_from_flat_file(year, path)
                if item:
                    items.append(item)
            elif path.is_dir():
                items.extend(self._items_from_case_dir(year, path))

        return items

    def _item_from_flat_file(self, year: str, path: Path) -> UploadItem | None:
        if path.suffix.lower() == ".pdf":
            case_id, lang = parse_pdf_member(path.name)
            return self._file_item(
                year=year,
                archive_type="pdf",
                path=path,
                s3_key=self._pdf_key(year, case_id, lang),
                content_type="application/pdf",
            )

        if path.suffix.lower() == ".json" and path.name != "metadata.json":
            case_id = path.stem
            return self._file_item(
                year=year,
                archive_type="metadata",
                path=path,
                s3_key=self._metadata_key(year, case_id),
                content_type="application/json",
            )

        if path.name == "metadata.json":
            logging.warning(
                "Skipping aggregate metadata file without case id: %s. "
                "Use per-case <CASE_ID>.json files or case/<metadata.json> folders.",
                path,
            )

        return None

    def _items_from_case_dir(self, year: str, case_dir: Path) -> list[UploadItem]:
        case_id = case_dir.name.removeprefix("case=")
        items: list[UploadItem] = []

        metadata_path = case_dir / "metadata.json"
        if metadata_path.exists():
            items.append(
                self._file_item(
                    year=year,
                    archive_type="metadata",
                    path=metadata_path,
                    s3_key=self._metadata_key(year, case_id),
                    content_type="application/json",
                )
            )

        pdf_dirs = [case_dir, case_dir / "pdfs"]
        for pdf_dir in pdf_dirs:
            if not pdf_dir.exists() or not pdf_dir.is_dir():
                continue
            for pdf_path in sorted(pdf_dir.glob("*.pdf")):
                lang = pdf_path.stem.upper()
                if "_" in pdf_path.stem and pdf_dir == case_dir:
                    parsed_case_id, parsed_lang = parse_pdf_member(pdf_path.name)
                    if parsed_case_id == case_id:
                        lang = parsed_lang
                items.append(
                    self._file_item(
                        year=year,
                        archive_type="pdf",
                        path=pdf_path,
                        s3_key=self._pdf_key(year, case_id, lang),
                        content_type="application/pdf",
                    )
                )

        return items

    def _file_item(
        self,
        *,
        year: str,
        archive_type: str,
        path: Path,
        s3_key: str,
        content_type: str,
    ) -> UploadItem:
        return UploadItem(
            year=year,
            source_type="file",
            archive_type=archive_type,
            source_path=str(path),
            member_name="",
            s3_key=s3_key,
            content_type=content_type,
            size=path.stat().st_size,
        )

    def process_item(self, item: UploadItem) -> tuple[UploadItem, Status]:
        if self.progress.already_done(item.s3_key):
            if self.object_exists(item.s3_key):
                log_skip(item.s3_key, "already recorded in progress")
                return item, "skipped"
            logging.info(
                "Progress recorded %s as done, but object is missing in S3; retrying",
                item.s3_key,
            )

        if self.dry_run:
            log_skip(item.s3_key, "dry-run")
            return item, "skipped"

        if self.object_exists(item.s3_key):
            log_skip(item.s3_key, "already exists in S3")
            self.progress.record(item, "skipped")
            return item, "skipped"

        last_error: str | None = None
        for attempt in range(1, self.retries + 1):
            try:
                self.upload_tar_member(item)
                log_upload(self.bucket, item.s3_key, f"{item.size} bytes")
                self.progress.record(item, "uploaded")
                return item, "uploaded"
            except Exception as e:
                last_error = str(e)
                if attempt < self.retries:
                    time.sleep(min(2**attempt, 30))
                else:
                    log_failed(item.s3_key, last_error)
                    self.progress.record(item, "failed", error=last_error)
                    return item, "failed"

        log_failed(item.s3_key, last_error or "unknown error")
        return item, "failed"

    def upload_tar_member(self, item: UploadItem) -> None:
        import tarfile

        if item.source_type == "file":
            validate_upload_source(item, Path(item.source_path))
            self.s3.upload_file(
                item.source_path,
                self.bucket,
                item.s3_key,
                ExtraArgs={"ContentType": item.content_type},
                Config=self.transfer_config,
            )
            return

        # Materialize the member into a temporary file so boto3 can use multipart
        # upload for large files through upload_file.
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
            with tarfile.open(item.source_path, "r") as tf:
                src = tf.extractfile(item.member_name)
                if src is None:
                    raise RuntimeError(f"Could not extract {item.member_name}")
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)

        try:
            validate_upload_source(item, tmp_path)
            self.s3.upload_file(
                str(tmp_path),
                self.bucket,
                item.s3_key,
                ExtraArgs={"ContentType": item.content_type},
                Config=self.transfer_config,
            )
        finally:
            tmp_path.unlink(missing_ok=True)

    def object_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def write_report(self) -> None:
        self.report_file.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(self.report)
        payload["generated_at"] = utc_now()
        payload["bucket"] = self.bucket
        payload["data_dir"] = str(self.data_dir)
        payload["prefix"] = self.prefix
        payload["progress_file"] = str(self.progress_file)
        payload["report_file"] = str(self.report_file)
        with self.report_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

    def _metadata_key(self, year: str, case_id: str) -> str:
        return f"{self.prefix}/year={year}/case={case_id}/metadata.json"

    def _pdf_key(self, year: str, case_id: str, lang: str) -> str:
        return f"{self.prefix}/year={year}/case={case_id}/pdfs/{lang}.pdf"


def parse_pdf_member(member_name: str) -> tuple[str, str]:
    stem = Path(member_name).stem
    if "_" not in stem:
        raise ValueError(f"PDF filename does not include language suffix: {member_name}")
    case_id, lang = stem.rsplit("_", 1)
    return case_id, lang.upper()


def dedupe_items(items: list[UploadItem]) -> list[UploadItem]:
    """Keep one upload item per S3 key.

    A year directory can contain both source tar archives and already-expanded
    files. Uploading the same key twice wastes S3 HEAD/PUT calls, so prefer
    direct file items over tar member items and keep discovery stable.
    """
    by_key: dict[str, UploadItem] = {}
    for item in items:
        existing = by_key.get(item.s3_key)
        if existing is None or (
            existing.source_type == "tar" and item.source_type == "file"
        ):
            by_key[item.s3_key] = item
    return list(by_key.values())


def validate_upload_source(item: UploadItem, path: Path) -> None:
    if item.content_type != "application/pdf":
        return

    with path.open("rb") as f:
        header = f.read(5)

    if not header.startswith(b"%PDF-"):
        raise ValueError(
            f"Invalid PDF content for {item.member_name or item.source_path}: "
            f"expected %PDF- header, got {header.hex() or 'empty'}"
        )


def resolve_state_files(
    years: list[str] | None,
    progress_file: str | None,
    report_file: str | None,
) -> tuple[Path, Path]:
    year_label = state_year_label(years)
    progress_path = Path(progress_file) if progress_file else Path(f"{year_label}_{DEFAULT_PROGRESS_SUFFIX}")
    report_path = Path(report_file) if report_file else Path(f"{year_label}_{DEFAULT_REPORT_SUFFIX}")
    return progress_path, report_path


def state_year_label(years: list[str] | None) -> str:
    if not years:
        return "all_years"

    unique_years = sorted(dict.fromkeys(years))
    if len(unique_years) == 1:
        return unique_years[0]

    return "years_" + "_".join(unique_years)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_upload(bucket: str, key: str, detail: str) -> None:
    logging.info("[UPLOAD] s3://%s/%s %s", bucket, key, detail)


def log_skip(key: str, reason: str) -> None:
    logging.info("[SKIP] %s (%s)", key, reason)


def log_failed(key: str, reason: str) -> None:
    logging.error("[FAILED] %s (%s)", key, reason)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload judgments to case-centric S3")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", str(DEFAULT_DATA_DIR)))
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", DEFAULT_PREFIX))
    parser.add_argument(
        "--progress-file",
        help=(
            "Progress file path. Defaults to <YEAR>_upload_progress.json "
            "for single-year runs."
        ),
    )
    parser.add_argument(
        "--report-file",
        help=(
            "Report file path. Defaults to <YEAR>_upload_report.json "
            "for single-year runs."
        ),
    )
    parser.add_argument("--workers", type=int, default=int(os.getenv("UPLOAD_WORKERS", "8")))
    parser.add_argument("--retries", type=int, default=int(os.getenv("UPLOAD_RETRIES", "3")))
    parser.add_argument("--year", action="append", help="Upload one year; repeat for multiple years")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    if not args.bucket:
        raise SystemExit("S3_BUCKET is required, or pass --bucket")

    progress_file, report_file = resolve_state_files(
        args.year,
        args.progress_file,
        args.report_file,
    )
    logging.info("Using progress file: %s", progress_file)
    logging.info("Using report file: %s", report_file)

    uploader = S3CaseUploader(
        bucket=args.bucket,
        region=args.region,
        prefix=args.prefix,
        data_dir=Path(args.data_dir),
        progress_file=progress_file,
        report_file=report_file,
        workers=args.workers,
        retries=args.retries,
        dry_run=args.dry_run,
    )
    report = uploader.run(years=args.year)
    logging.info("Upload report: %s", json.dumps(asdict(report), sort_keys=True))
    return 0 if report.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
