#!/usr/bin/env python3
"""Simple Kaggle dataset downloader.

Downloads the listed datasets into `data/input`. This script assumes
dependencies are handled by the project's `requirements.txt` (e.g.
`pip install -r requirements.txt`).
"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "input"

DATASETS = [
    "gsimonx37/letterboxd",
    "akshaypawar7/millions-of-movies",
]


def main():
    try:
        import kagglehub
    except Exception:
        print("`kagglehub` is not installed. Run: pip install -r requirements.txt")
        sys.exit(2)

    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    for ds in DATASETS:
        print(f"Downloading {ds} into {INPUT_DIR} (skips if already present)...")
        try:
            # ask kagglehub to download and unzip into the input dir
            kagglehub.dataset_download(ds, path=str(INPUT_DIR), unzip=True)
        except TypeError:
            # some versions may not accept unzip kwarg
            kagglehub.dataset_download(ds, path=str(INPUT_DIR))
        except Exception as e:
            print(f"Failed to download {ds}: {e}")

    print("\nDone. Check `data/input` for downloaded datasets.")


if __name__ == "__main__":
    main()
