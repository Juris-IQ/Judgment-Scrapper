#!/usr/bin/env python3
"""Recover Delhi metadata JSON files that are outside the public tar index."""

from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--missing-list", type=Path, default=Path("data/delhi-high-court/missing_metadata_pdfs.txt"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/aws-public/delhi-loose-metadata"))
    parser.add_argument("--bucket", default="indian-high-court-judgments")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--court", default="7_26")
    parser.add_argument("--bench", default="dhcdb")
    parser.add_argument("--workers", type=int, default=16)
    return parser.parse_args()


def target_for_pdf(pdf_name: str, court: str, bench: str) -> tuple[str, Path]:
    stem = Path(pdf_name).stem
    match = re.search(r"_(\d{4})-\d{2}-\d{2}$", stem)
    if not match:
        raise ValueError(f"Cannot determine decision year from {pdf_name}")
    year = match.group(1)
    key = f"metadata/json/year={year}/court={court}/bench={bench}/{stem}.json"
    return key, Path(year) / f"court={court}" / f"bench={bench}" / f"{stem}.json"


def main() -> int:
    args = parse_args()
    pdf_names = sorted({line.strip() for line in args.missing_list.read_text(encoding="utf-8").splitlines() if line.strip()})
    s3 = boto3.client("s3", region_name=args.region, config=Config(signature_version=UNSIGNED))
    tasks = [(pdf_name, *target_for_pdf(pdf_name, args.court, args.bench)) for pdf_name in pdf_names]
    report = {"requested": len(tasks), "recovered": 0, "missing": 0, "failed": []}

    def recover(task: tuple[str, str, Path]) -> tuple[str, str]:
        pdf_name, key, relative_path = task
        destination = args.output_dir / relative_path
        if destination.exists():
            return "already_present", pdf_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            response = s3.get_object(Bucket=args.bucket, Key=key)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return "missing", pdf_name
            raise
        destination.write_bytes(response["Body"].read())
        json.loads(destination.read_text(encoding="utf-8"))
        return "recovered", pdf_name

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(recover, task) for task in tasks]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Recovering metadata", unit="file"):
            try:
                status, pdf_name = future.result()
                if status in {"recovered", "already_present"}:
                    report["recovered"] += 1
                else:
                    report["missing"] += 1
            except Exception as error:
                report["failed"].append(str(error))

    report_path = args.output_dir / "recovery_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
