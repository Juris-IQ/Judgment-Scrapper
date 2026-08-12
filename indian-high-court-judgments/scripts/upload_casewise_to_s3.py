#!/usr/bin/env python3
"""Upload the case-wise High Court layout to S3.

Local layout:
    data/high-court/year=YYYY/court=19_16/bench=*/case=CNR/
        metadata.json
        pdfs/*.pdf

S3 layout:
    high-court/year=YYYY/bench=*/case=CNR/
        metadata.json
        pdfs/*.pdf

The metadata uploaded to S3 rewrites each document's ``pdf_link`` to a
relative link rooted at the ``high-court`` prefix, for example:

    /year=1950/bench=calcutta_original_side/
    case=WBCHCO0000011947/pdfs/document.pdf

This script plans by default. Pass ``--apply`` to perform the upload.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from tqdm import tqdm


DEFAULT_DATA_DIR = Path("data/high-court")
DEFAULT_PREFIX = "high-court"


@dataclass(frozen=True)
class CaseRef:
    path: Path
    year: str
    court: str
    bench: str
    case_id: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-1"))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", DEFAULT_PREFIX))
    parser.add_argument("--year", action="append", help="Limit upload; repeat for multiple years")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true", help="Actually upload to S3")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing S3 objects")
    parser.add_argument("--report", type=Path, default=Path("data/high-court/upload_report.json"))
    return parser.parse_args()


def discover_cases(data_dir: Path, years: set[str] | None) -> list[CaseRef]:
    cases: list[CaseRef] = []
    for metadata_path in data_dir.glob("year=*/court=*/bench=*/case=*/metadata.json"):
        case_dir = metadata_path.parent
        parts = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in case_dir.parts if "=" in part}
        if not {"year", "court", "bench", "case"}.issubset(parts):
            logging.warning("Skipping unexpected case path: %s", case_dir)
            continue
        if years and parts["year"] not in years:
            continue
        cases.append(CaseRef(case_dir, parts["year"], parts["court"], parts["bench"], parts["case"]))
    return sorted(cases, key=lambda item: (item.year, item.court, item.bench, item.case_id))


def relative_pdf_link(case: CaseRef, filename: str) -> str:
    return (
        f"/year={case.year}/bench={case.bench}/"
        f"case={case.case_id}/pdfs/{filename}"
    )


def s3_key(prefix: str, relative_link: str) -> str:
    return f"{prefix.strip('/')}{relative_link}"


def load_uploaded_metadata(case: CaseRef, prefix: str) -> tuple[dict[str, Any], bytes]:
    metadata_path = case.path / "metadata.json"
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    documents = payload.get("documents", [])
    if not isinstance(documents, list):
        raise ValueError(f"documents must be a list: {metadata_path}")

    for document in documents:
        filename = document.get("filename") or Path(document.get("path", "")).name
        if not filename:
            raise ValueError(f"Document has no filename: {metadata_path}")
        link = relative_pdf_link(case, filename)
        document["path"] = f"pdfs/{filename}"
        document["pdf_link"] = link
        document["s3_key"] = s3_key(prefix, link)

    payload["s3_prefix"] = prefix.strip("/")
    payload["s3_metadata_key"] = f"{prefix.strip('/')}/year={case.year}/bench={case.bench}/case={case.case_id}/metadata.json"
    return payload, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def process_case(case: CaseRef, *, s3, bucket: str, prefix: str, apply: bool, overwrite: bool, transfer_config: TransferConfig) -> dict[str, Any]:
    payload, metadata_body = load_uploaded_metadata(case, prefix)
    metadata_key = f"{prefix.strip('/')}/year={case.year}/bench={case.bench}/case={case.case_id}/metadata.json"
    pdfs = sorted((case.path / "pdfs").glob("*.pdf"))
    uploaded = skipped = 0

    if apply:
        if overwrite or not object_exists(s3, bucket, metadata_key):
            s3.put_object(Bucket=bucket, Key=metadata_key, Body=metadata_body, ContentType="application/json")
            uploaded += 1
        else:
            skipped += 1

    for pdf_path in pdfs:
        link = relative_pdf_link(case, pdf_path.name)
        key = s3_key(prefix, link)
        if apply:
            if overwrite or not object_exists(s3, bucket, key):
                s3.upload_file(
                    str(pdf_path),
                    bucket,
                    key,
                    ExtraArgs={"ContentType": "application/pdf"},
                    Config=transfer_config,
                )
                uploaded += 1
            else:
                skipped += 1

    return {"case": case.case_id, "year": case.year, "pdfs": len(pdfs), "uploaded": uploaded, "skipped": skipped}


def main() -> int:
    args = parse_args()
    if args.apply and not args.bucket:
        raise SystemExit("--bucket or S3_BUCKET is required with --apply")
    if not args.data_dir.exists():
        raise SystemExit(f"Case-wise data directory does not exist: {args.data_dir}")

    years = set(args.year) if args.year else None
    cases = discover_cases(args.data_dir, years)
    pdf_count = sum(len(list((case.path / "pdfs").glob("*.pdf"))) for case in cases)
    logging.info("Discovered %d case folder(s) and %d PDF(s)", len(cases), pdf_count)
    logging.info("S3 destination: s3://%s/%s/", args.bucket or "<bucket-required-for-apply>", args.prefix.strip("/"))

    if not cases:
        return 0

    if not args.apply:
        sample = cases[0]
        sample_pdf = next((p for p in (sample.path / "pdfs").glob("*.pdf")), None)
        print("DRY RUN: no S3 writes performed")
        print(f"Case sample: {sample.case_id}")
        if sample_pdf:
            link = relative_pdf_link(sample, sample_pdf.name)
            print(f"pdf_link: {link}")
            print(f"s3_key: {s3_key(args.prefix, link)}")
        return 0

    s3 = boto3.client("s3", region_name=args.region)
    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True,
    )
    report = {"cases": len(cases), "pdfs": pdf_count, "uploaded_objects": 0, "skipped_objects": 0, "failed_cases": []}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                process_case,
                case,
                s3=s3,
                bucket=args.bucket,
                prefix=args.prefix,
                apply=True,
                overwrite=args.overwrite,
                transfer_config=transfer_config,
            )
            for case in cases
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading cases", unit="case"):
            try:
                result = future.result()
                report["uploaded_objects"] += result["uploaded"]
                report["skipped_objects"] += result["skipped"]
            except Exception as error:
                report["failed_cases"].append(str(error))

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 1 if report["failed_cases"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
