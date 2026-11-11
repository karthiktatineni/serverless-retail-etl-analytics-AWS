
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def make(
    day: str,
    rows: int = 5000,
    seed: int | None = None,
    defect_rate: float = 0.01,
) -> pd.DataFrame:
    datetime.strptime(day, "%Y-%m-%d")

    if rows < 1:
        raise ValueError("rows must be greater than zero")

    if not 0.0 <= defect_rate <= 0.20:
        raise ValueError("defect_rate must be between 0 and 0.20")

    k = int(rows * defect_rate)
    actual_seed = seed if seed is not None else int(day.replace("-", ""))
    rng = np.random.default_rng(actual_seed)

    categories = [
        "Electronics", "Fashion", "Home",
        "Beauty", "Sports", "Books"
    ]

    df = pd.DataFrame({
        "order_id": [
            f"{day.replace('-', '')}-{i:06d}"
            for i in range(rows)
        ],
        "customer_id": rng.integers(
            1000, 9000, rows
        ).astype(float),
        "category": rng.choice(categories, rows),
        "quantity": rng.integers(1, 5, rows),
        "unit_price": rng.uniform(5, 300, rows).round(2),
        "order_ts": (
            pd.Timestamp(day)
            + pd.to_timedelta(
                rng.integers(0, 86400, rows),
                unit="s"
            )
        ),
    })

    # Select four separate groups so defects do not overlap.
    selected = rng.permutation(rows)[:4 * k]

    missing_customer = selected[:k]
    bad_quantity = selected[k:2 * k]
    bad_price = selected[2 * k:3 * k]
    missing_timestamp = selected[3 * k:4 * k]

    df.loc[missing_customer, "customer_id"] = np.nan
    df.loc[bad_quantity, "quantity"] = -1
    df.loc[bad_price, "unit_price"] = 0
    df.loc[missing_timestamp, "order_ts"] = pd.NaT

    # Duplicate only otherwise-valid rows.
    valid_indices = np.setdiff1d(
        np.arange(rows), selected, assume_unique=True
    )

    if k:
        duplicate_indices = rng.choice(
            valid_indices, size=k, replace=False
        )
        duplicates = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Generate simulated daily store orders."
    )

    parser.add_argument(
        "day",
        nargs="?",
        default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--defect-rate", type=float, default=0.01)

    args = parser.parse_args()

    try:
        df = make(
            day=args.day,
            rows=args.rows,
            seed=args.seed,
            defect_rate=args.defect_rate,
        )
    except ValueError as exc:
        parser.error(str(exc))

    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"orders_{args.day}.csv"

    df.to_csv(output_file, index=False)

    k = int(args.rows * args.defect_rate)
    expected_rejected = 5 * k

    print(f"Saved file: {output_file}")
    print(f"Total rows: {len(df):,}")
    print(f"Expected rejected rows: {expected_rejected:,}")
    print(
        f"Expected reject rate: "
        f"{expected_rejected / len(df):.2%}"
    )


if __name__ == "__main__":
    main()
