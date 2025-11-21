#!/usr/bin/env python3
"""
Create a Yelp JSONL file with synthetic duplicates for Bloom filter testing.

Reads an existing small Yelp sample (default: data/landing/yelp_small.jsonl),
injects exact duplicates for a subset of reviews, and writes a new JSONL file
that the Kafka producer can replay.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import random
from typing import Iterable, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a Yelp JSONL file with synthetic duplicate reviews."
    )
    parser.add_argument(
        "--source",
        type=pathlib.Path,
        default=pathlib.Path("data/landing/yelp_small.jsonl"),
        help="Path to the base Yelp JSONL file (assumed to have unique reviews).",
    )
    parser.add_argument(
        "--destination",
        type=pathlib.Path,
        default=pathlib.Path("data/landing/yelp_small_bf.jsonl"),
        help="Output path for the JSONL file with duplicates.",
    )
    parser.add_argument(
        "--duplicate-fraction",
        type=float,
        default=0.10,
        help=(
            "Fraction of the base reviews to duplicate (e.g., 0.10 = 10%% of rows "
            "will be selected and copied once more)."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Random seed for selecting which reviews to duplicate.",
    )
    return parser.parse_args()


def load_reviews(src: pathlib.Path) -> List[str]:
    if not src.exists():
        raise SystemExit(f"Source file not found: {src}")

    lines: List[str] = []
    with src.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Validate JSON row
            json.loads(line)
            lines.append(line)

    if not lines:
        raise SystemExit(f"No valid JSON lines found in source file: {src}")

    return lines


def add_duplicates(
    base_reviews: Iterable[str], duplicate_fraction: float, seed: int
) -> List[str]:
    reviews = list(base_reviews)
    n = len(reviews)

    if duplicate_fraction <= 0.0:
        # No duplicates requested; just return the original list
        return reviews

    rng = random.Random(seed)

    num_dups = max(1, int(n * duplicate_fraction))
    dup_indices = [rng.randrange(n) for _ in range(num_dups)]

    # Start with original reviews, then append exact duplicates
    output: List[str] = reviews.copy()
    for idx in dup_indices:
        output.append(reviews[idx])

    # Shuffle so duplicates are interleaved rather than all at the end
    rng.shuffle(output)
    return output


def main() -> None:
    args = parse_args()

    if args.duplicate_fraction < 0.0:
        raise SystemExit("--duplicate-fraction must be >= 0.0")

    args.destination.parent.mkdir(parents=True, exist_ok=True)

    base_reviews = load_reviews(args.source)
    output_reviews = add_duplicates(
        base_reviews, duplicate_fraction=args.duplicate_fraction, seed=args.seed
    )

    with args.destination.open("w") as dst:
        for line in output_reviews:
            dst.write(line + "\n")

    num_base = len(base_reviews)
    num_output = len(output_reviews)
    num_dups = num_output - num_base

    print(
        f"Wrote {num_output:,} reviews to {args.destination} "
        f"({num_base:,} base + {num_dups:,} exact duplicates, "
        f"duplicate_fraction={args.duplicate_fraction})"
    )


if __name__ == "__main__":
    main()


