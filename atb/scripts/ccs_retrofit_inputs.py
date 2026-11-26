"""Build the ReEDS inputs/upgrades CCS retrofit files from the ATB flat file.

Writes ``upgrade_mult_atb<yy>_ccs_{mid,adv,con}.csv`` (each scenario's retrofit
"Additional OCC" divided by the Moderate value in ``normalization_year``; this
reproduces the ATB 2023 files in ReEDS exactly) and
``upgrade_costs_ccs_{coal,gas}.csv`` (the same series in 2004$/kW). Mappings
live under ``ccs_retrofits`` in ``settings.yaml``; see ../README.md.
"""

import argparse
import os
import re
import shutil
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "helpers"))

from atb_config import ATBE_COLUMN_MAPPING, load_processing_settings  # noqa: E402


CSV_LINE_TERMINATOR = "\n"
FLAT_FILE_COLUMNS = [
    "technology", "display_name", "core_metric_parameter", "core_metric_case",
    "crpyears", "scenario", "core_metric_variable", "value",
]


def read_retrofit_costs(settings):
    """Return retrofit costs as a frame indexed by (fuel, Scenario) with year columns."""
    rules = settings["ccs_retrofits"]
    case = settings["config"]["processing"]["atb_case"]
    technologies = {fuel["Technology"] for fuel in rules["fuels"].values()}
    path = settings["flat_file_path"]
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Raw ATB flat file not found: {path}\n"
            "Run 'python scripts/future_atb_scraper.py' first."
        )
    print(f"Loading retrofit rows from {path}")
    chunks = pd.read_csv(path, usecols=FLAT_FILE_COLUMNS, chunksize=500_000, low_memory=False)
    rows = pd.concat(
        [chunk[chunk["technology"].isin(technologies)] for chunk in chunks],
        ignore_index=True,
    ).rename(columns=ATBE_COLUMN_MAPPING)
    rows = rows[(rows["Parameter"] == rules["parameter"]) & (rows["Case"] == case)]
    if rows.empty:
        raise ValueError(
            f"No {rules['parameter']!r} rows for {sorted(technologies)} in case {case!r}."
        )

    series = []
    for fuel, mapping in rules["fuels"].items():
        selected = rows[
            (rows["Technology"] == mapping["Technology"])
            & (rows["DisplayName"] == mapping["DisplayName"])
        ]
        if selected.empty:
            available = sorted(rows.loc[rows["Technology"] == mapping["Technology"], "DisplayName"].unique())
            raise ValueError(
                f"{mapping['DisplayName']!r} not found for {fuel}; available: {available}."
            )
        # The retrofit cost does not depend on the capital recovery period;
        # make sure before collapsing it.
        by_crp = selected.pivot_table(
            index=["Scenario", "variable"], columns="CRPYears", values="value"
        )
        if (by_crp.nunique(axis=1) > 1).any():
            raise ValueError(f"{rules['parameter']} for {fuel} differs across CRPYears.")
        wide = selected[selected["CRPYears"] == selected["CRPYears"].min()].pivot_table(
            index="Scenario", columns="variable", values="value"
        )
        wide.index = pd.MultiIndex.from_product([[fuel], wide.index], names=["fuel", "Scenario"])
        series.append(wide)
    costs = pd.concat(series)
    costs.columns = costs.columns.astype(int)
    return costs


def extend_years(costs, start_year, end_year):
    """Hold the first ATB year flat back to start_year; never extrapolate forward."""
    first, last = costs.columns.min(), costs.columns.max()
    if end_year > last:
        raise ValueError(
            f"ATB retrofit costs end in {last} but processing.reeds_end_year is {end_year}."
        )
    years = list(range(start_year, end_year + 1))
    out = pd.DataFrame(index=costs.index, columns=years, dtype=float)
    for year in years:
        out[year] = costs[max(year, first)]
    return out


def build_multipliers(costs, settings):
    """Return {scenario: frame} of multipliers on the ReEDS upgrade_link rows."""
    rules = settings["ccs_retrofits"]
    norm_year = int(rules["normalization_year"])
    norm_scenario = rules["normalization_scenario"]
    link = pd.read_csv(os.path.join(settings["reedspath"], "inputs", "upgrades", "upgrade_link.csv"))
    rows = link["*TO"].tolist()
    deltas = link.set_index("*TO")["DELTA"]

    multipliers = {}
    for scenario in costs.index.get_level_values("Scenario").unique():
        table = pd.DataFrame(1.0, index=pd.Index(rows, name="dummy"), columns=costs.columns)
        for fuel, mapping in rules["fuels"].items():
            ratio = costs.loc[(fuel, scenario)] / costs.loc[(fuel, norm_scenario), norm_year]
            pattern = re.compile(mapping["delta_pattern"])
            matched = [row for row in rows if pattern.search(deltas[row])]
            if not matched:
                raise ValueError(f"No upgrade_link rows match {mapping['delta_pattern']!r} for {fuel}.")
            table.loc[matched] = ratio.values
        multipliers[scenario] = table
    return multipliers


def build_costs(costs, settings):
    """Return {fuel: frame} of retrofit costs in 2004$/kW, one column per scenario."""
    deflator = pd.read_csv(
        os.path.join(settings["reedspath"], "inputs", "financials", "deflator.csv"),
        index_col="*Dollar.Year",
    ).squeeze()
    atb_dollar_year = int(settings["dollaryear"])
    to_2004 = deflator[atb_dollar_year] / deflator[2004]
    column_prefix = f"atb{str(settings['atbyear'])[-2:]}"
    scenarios = list(settings["ccs_retrofits"]["scenario_suffix"])
    tables = {}
    for fuel in settings["ccs_retrofits"]["fuels"]:
        table = (costs.loc[fuel].reindex(scenarios) * to_2004).T
        table.columns = [f"{column_prefix}_{scenario.lower()}" for scenario in table.columns]
        table.index.name = "t"
        tables[fuel] = table
    return tables


def write_outputs(multipliers, cost_tables, settings, copy_to_reeds):
    """Write the multiplier and cost tables to the output folder and optionally copy them to ReEDS."""
    rules = settings["ccs_retrofits"]
    outfolder = settings["output_dir"]
    os.makedirs(outfolder, exist_ok=True)
    yy = str(settings["atbyear"])[-2:]
    decimals = settings["decimals"]
    written = []
    for scenario, table in multipliers.items():
        filename = f"upgrade_mult_atb{yy}_ccs_{rules['scenario_suffix'][scenario]}.csv"
        table.round(decimals).to_csv(os.path.join(outfolder, filename), lineterminator=CSV_LINE_TERMINATOR)
        written.append(filename)
    for fuel, table in cost_tables.items():
        filename = f"upgrade_costs_ccs_{fuel}.csv"
        table.round(decimals).to_csv(os.path.join(outfolder, filename), lineterminator=CSV_LINE_TERMINATOR)
        written.append(filename)
    for filename in written:
        print(f"Saved {os.path.join(outfolder, filename)}")
    if copy_to_reeds:
        target = os.path.join(settings["reedspath"], "inputs", "upgrades")
        for filename in written:
            shutil.copy(os.path.join(outfolder, filename), os.path.join(target, filename))
            print(f"Copied {filename} to {target}")
    return written


def main():
    """Build the CCS retrofit multiplier and cost files from the configured ATB flat file."""
    parser = argparse.ArgumentParser(description="Build ReEDS CCS retrofit upgrade inputs from ATB.")
    parser.add_argument("--config", help="path to config.yaml (default: ../config.yaml)")
    parser.add_argument(
        "--copy-to-reeds", action="store_true",
        help="also copy the outputs into <reeds_repo>/inputs/upgrades (default: processing.copy_to_reeds)",
    )
    args = parser.parse_args()
    settings = load_processing_settings(args.config)
    if "ccs_retrofits" not in settings:
        raise KeyError("settings.yaml has no ccs_retrofits section.")

    costs = read_retrofit_costs(settings)
    print("ATB retrofit cost rows (first/last year):", costs.columns.min(), costs.columns.max())
    end_year = settings["reeds_end_year"] or int(costs.columns.max())
    costs = extend_years(costs, settings["reeds_start_year"], end_year)
    multipliers = build_multipliers(costs, settings)
    cost_tables = build_costs(costs, settings)
    write_outputs(multipliers, cost_tables, settings, args.copy_to_reeds or settings["copy_to_reeds"])


if __name__ == "__main__":
    main()
