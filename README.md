# Juris-IQ Judgment Scrapers

This repository contains the Juris-IQ scrapers and data-processing tools for Indian court judgments.

It is organized by court so that each scraper can be developed and run independently while sharing the same overall workflow: collect judgment metadata and PDFs, validate the local dataset, and publish structured case-wise data to Amazon S3.

## Repository layout

```text
Judgment-Scrapper/
├── indian-high-court-judgments/
│   ├── download.py
│   ├── process_metadata.py
│   ├── scripts/
│   ├── src/
│   └── tests/
└── indian-supreme-court-judgments/
    ├── download.py
    ├── process_metadata.py
    ├── archive_manager.py
    └── sync_s3.py
```

## High Court scraper

The High Court scraper downloads judgment records from the eCourts judgments portal. It supports court/date ranges, resumable downloads, CAPTCHA solving, metadata processing, and optional PDF compression.

```bash
cd indian-high-court-judgments
python download.py --court_code "7~26" --start_date 1950-01-01 --end_date 2025-12-31
```

Court codes use the eCourts format `STATE~COURT`; the S3 format uses an underscore, for example Delhi `7~26` becomes `7_26`.

The public AWS Open Data dataset is usually preferable for bulk downloads:

```bash
aws s3 sync s3://indian-high-court-judgments/data/tar/ ./data/tar/ \
  --exclude "*" --include "*/court=7_26/*" --no-sign-request
```

### Case-wise High Court layout

The case-wise tools convert AWS tar archives into this local structure:

```text
data/<court>-high-court/
└── year=YYYY/
    └── court=XX_YY/
        └── bench=<bench>/
            └── case=<CNR>/
                ├── metadata.json
                └── pdfs/<document>.pdf
```

The uploader publishes the court-specific S3 prefix without repeating the court code:

```text
s3://jurisiq-sc-judgements/high-court/<court-name>/
└── year=YYYY/bench=<bench>/case=<CNR>/
    ├── metadata.json
    └── pdfs/<document>.pdf
```

Example upload:

```bash
python scripts/upload_casewise_to_s3.py \
  --data-dir data/delhi-high-court \
  --bucket jurisiq-sc-judgements \
  --region ap-south-1 \
  --prefix high-court/delhi-high-court \
  --workers 16 \
  --apply
```

The uploader is a dry run by default. Add `--apply` only when the local case-wise dataset has been audited. Do not use `--overwrite` for resumable uploads.

## Supreme Court scraper

The Supreme Court scraper downloads judgments from the Supreme Court eCourts portal and supports metadata processing, archive creation, and S3 synchronization. See [`indian-supreme-court-judgments/README.md`](indian-supreme-court-judgments/README.md) for its data layout and commands.

## Data and generated files

Downloaded PDFs, raw metadata, tar archives, virtual environments, CAPTCHA artifacts, caches, and temporary files are intentionally excluded from Git. The repository contains code and documentation; bulk data belongs in the configured S3 datasets.

The CAPTCHA ONNX model is also excluded because it is a large runtime artifact. To run the High Court scraper, place the compatible model at `indian-high-court-judgments/src/captcha_solver/captcha.onnx`; obtain it from the approved project storage or the upstream scraper distribution.

## Responsible use

The court portals are public services. Use moderate concurrency, respect rate limits, and prefer the public AWS datasets for bulk access when available.

## License and attribution

The upstream datasets and portal-derived metadata may have their own licensing and attribution requirements. See the individual scraper README files and upstream dataset documentation before redistribution.
