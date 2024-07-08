# FOS scraper

Use this script to download the metadata (and PDFs) of the Financial Ombudsman Decisions.

## Download metadata

```
%run scrape.py get-metadata
```

By default, this will download the last 6 months of decisions, modify the `--from` argument.

This scripts creates a file named `metadata.csv` which contains metadata associated to the decisions the FOS has made.

## Download decision PDFs

```
%run scrape.py download-decisions
```

This script reads the `metadata.csv` and downloads the PDFs to the `decisions` folder.
