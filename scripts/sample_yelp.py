#!/usr/bin/env python3
"""
Sample a manageable subset from the Yelp Academic Dataset review JSON.

The script emits JSON Lines (one review per line) to a target file that the
Kafka producer can replay (see producer/send_reviews.py).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import random
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a smaller JSONL file from the Yelp reviews dump."
    )
    parser.add_argument(
        "--source",
        type=pathlib.Path,
        default=pathlib.Path("yelp_academic_dataset_review.json"),
        help="Path to the full Yelp reviews JSON file.",
    )
    parser.add_argument(
        "--destination",
        type=pathlib.Path,
        default=pathlib.Path("data/landing/yelp_small.jsonl"),
        help="Path for the sampled JSONL output.",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=100_000,
        help="Number of reviews to keep in the sampled file.",
    )
    parser.add_argument(
        "--method",
        choices=("head", "reservoir"),
        default="head",
        help=(
            "'head' takes the first N rows; "
            "'reservoir' produces a uniform random sample of N rows."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Random seed used when --method=reservoir.",
    )
    return parser.parse_args()


def head_sample(lines: Iterable[str], n: int) -> list[str]:
    sample = []
    for line in lines:
        if not line.strip():
            continue
        sample.append(line.rstrip("\n"))
        if len(sample) >= n:
            break
    return sample


def reservoir_sample(lines: Iterable[str], n: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    sample: list[str] = []
    for idx, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        if len(sample) < n:
            sample.append(line)
            continue
        j = rng.randint(1, idx)
        if j <= n:
            sample[j - 1] = line
    return sample


def main() -> None:
    args = parse_args()
    if args.records <= 0:
        raise SystemExit("--records must be a positive integer")

    if not args.source.exists():
        raise SystemExit(f"Source file not found: {args.source}")

    args.destination.parent.mkdir(parents=True, exist_ok=True)

    with args.source.open() as src:
        if args.method == "head":
            sample = head_sample(src, args.records)
        else:
            sample = reservoir_sample(src, args.records, args.seed)

    if not sample:
        raise SystemExit("No data was sampled; check source file contents.")

    with args.destination.open("w") as dst:
        for line in sample:
            json.loads(line)  # validate JSON row
            dst.write(line + "\n")

    print(
        f"Wrote {len(sample):,} reviews to {args.destination} "
        f"(method={args.method}, source={args.source})"
    )


if __name__ == "__main__":
    main()

