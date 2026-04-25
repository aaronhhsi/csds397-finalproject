"""
Run the full pipeline locally without Airflow.

Usage:
    python run_pipeline.py            # full run
    python run_pipeline.py --step 3  # resume from step 3
    python run_pipeline.py --dry     # print step plan only

Steps:
  1  init_db
  2  ingest_places
  3  ingest_chr
  4  ingest_urban_rural
  5  clean_places
  6  clean_chr
  7  clean_urban_rural
  8  merge_transform
  9  run_analysis
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database.db_utils import init_db
from scripts.ingest_places      import run as ingest_places
from scripts.ingest_chr         import run as ingest_chr
from scripts.ingest_urban_rural import run as ingest_urban_rural
from scripts.clean_places       import clean_places
from scripts.clean_chr          import clean_chr
from scripts.clean_urban_rural  import clean_urban_rural
from scripts.merge_transform    import run as merge_transform
from analysis.hypotheses        import run_all as run_analysis

STEPS = [
    (1,  "init_db",            init_db),
    (2,  "ingest_places",      ingest_places),
    (3,  "ingest_chr",         ingest_chr),
    (4,  "ingest_urban_rural", ingest_urban_rural),
    (5,  "clean_places",       clean_places),
    (6,  "clean_chr",          clean_chr),
    (7,  "clean_urban_rural",  clean_urban_rural),
    (8,  "merge_transform",    merge_transform),
    (9,  "run_analysis",       run_analysis),
]


def main():
    parser = argparse.ArgumentParser(description="Run health data pipeline")
    parser.add_argument("--step", type=int, default=1,
                        help="Start from this step number (default: 1)")
    parser.add_argument("--only", type=int, default=None,
                        help="Run only this step number")
    parser.add_argument("--dry", action="store_true",
                        help="Print steps without running them")
    args = parser.parse_args()

    to_run = [(n, name, fn) for n, name, fn in STEPS
              if (args.only and n == args.only) or
                 (not args.only and n >= args.step)]

    if args.dry:
        print("Steps to run:")
        for n, name, _ in to_run:
            print(f"  {n:2d}. {name}")
        return

    total_start = time.time()
    for n, name, fn in to_run:
        print(f"\n{'─'*60}")
        print(f"Step {n}: {name}")
        print(f"{'─'*60}")
        t0 = time.time()
        fn()
        elapsed = time.time() - t0
        print(f"✓ {name} completed in {elapsed:.1f}s")

    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"Pipeline finished in {total:.1f}s")
    print(f"Run the dashboard:  python dashboard/app.py")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
