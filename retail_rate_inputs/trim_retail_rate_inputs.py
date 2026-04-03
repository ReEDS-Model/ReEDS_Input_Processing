"""
This script trims the three large CSV input files derived from FERC Form 1
(see ReadMe.md).

Usage:
    python trim_retail_rate_inputs.py

For each file the script reads from the Inputs/ folder and writes a trimmed
version with the same filename to the Outputs-trimmed/ folder.

What each trim does (based on usage in ferc_distadmin.py in the ReEDS
repository):

1. Electric Plant in Service
   - Rows:    keep only Account Classification == 'Additions'
   - Columns: Year, Utility Name, State,
              Trn - Total Transmission Plant,
              Dis - Total Distribution Plant,
              Gen - Total General Plant

2. Electric O & M Expenses
   - Rows:    all kept (no row filter)
   - Columns: Year, Utility Name, State, plus the 11 expense columns used by
              get_ferc_costs() and get_excluded_costs()

3. Electric Operating Revenues
   - Rows:    all kept (no row filter)
   - Columns: Year, Utility Name, State,
              Total Retail Sales MWh, Total Electricity Customers
"""

import os
import pandas as pd

# Set up directories
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "Inputs")
output_dir = os.path.join(script_dir, "Outputs-trimmed")
os.makedirs(output_dir, exist_ok=True)

# Define the files to trim, with their respective column and row filters
files = [
    {
        "name": "Electric Plant in Service-IOU-1993-2019",
        "keep_columns": [
            "Year",
            "Utility Name",
            "State",
            "Trn - Total Transmission Plant",
            "Dis - Total Distribution Plant",
            "Gen - Total General Plant",
        ],
        # Keep only the 'Additions' classification
        "row_filter": lambda df: df[df["Account Classification"] == "Additions"],
    },
    {
        "name": "Electric O & M Expenses-IOU-1993-2019",
        "keep_columns": [
            "Year",
            "Utility Name",
            "State",
            # -- used by get_ferc_costs() in ferc_distadmin.py --
            "Trn Total Operation Expenses $",
            "Trn Total Maintenance Expenses $",
            "Dis Total Maintenance Expenses $",
            "Dis Total Operation Expenses $",
            "Total Sales Expenses $",
            "Total Customer Srv & Information Expenses $",
            "CAE Total Customer Accounts Expenses $",
            "Total Admin & General Expenses $",
            "Total Regional Trans & Mark Operation Exps  $",
            "A&G Total Operation Expenses $",
            # -- used by get_excluded_costs() via excludecells in ferc_distadmin.py --
            "A&G Oper Injuries & Damages $",
        ],
        # all rows kept (no row filter)
        "row_filter": None,
    },
    {
        "name": "Electric Operating Revenues-IOU-1993-2019",
        "keep_columns": [
            "Year",
            "Utility Name",
            "State",
            "Total Retail Sales MWh",
            "Total Electricity Customers",
        ],
        # all rows kept (no row filter)
        "row_filter": None,
    },
]


def trim_file(file_spec):
    """Read, filter, and write a single trimmed csv file."""
    input_path = os.path.join(input_dir, f"{file_spec['name']}.csv")
    output_path = os.path.join(output_dir, f"{file_spec['name']}.csv")

    print(f"\n{'─' * 60}")
    print(f"Reading {file_spec['name']}.csv ...")
    df = pd.read_csv(input_path, encoding="latin1")
    print(f"  Original shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

    if file_spec["row_filter"] is not None:
        df = file_spec["row_filter"](df)

    df = df[file_spec["keep_columns"]]
    print(f"  Trimmed shape:  {df.shape[0]:,} rows x {df.shape[1]} columns")

    df.to_csv(output_path, index=False)

    original_mb = os.path.getsize(input_path) / 1_000_000
    trimmed_mb = os.path.getsize(output_path) / 1_000_000
    print(f"  Original size:  {original_mb:.1f} MB")
    print(f"  Trimmed size:   {trimmed_mb:.3f} MB")
    print(f"  Reduction:      {(1 - trimmed_mb / original_mb) * 100:.1f}%")
    print(f"  Wrote {output_path}")


def main():
    for file_spec in files:
        trim_file(file_spec)
    print(f"\n{'─' * 60}")
    print(f"Done. Trimmed files written to {output_dir}.")


if __name__ == "__main__":
    main()
