"""
Format ATB geothermal costs for ReEDS. See README for details.
"""

#%% ===========================================================================
### --- IMPORTS ---
### ===========================================================================
import argparse
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

#%% ===========================================================================
### --- CONSTANTS ---
### ===========================================================================
THISDIR = Path(__file__).resolve().parent
OUTPUTDIR = THISDIR / "output"

ATB_SCENARIOS = ["conservative", "moderate", "advanced"]
# full year range projected for ReEDS
YEAR_RANGE = range(2010, 2061)
# year the moderate/advanced cases reach their workbook target value
DECLINE_START_YEAR = 2035
# annual cost decline applied to all cases after DECLINE_START_YEAR
IMPROVEMENT_FACTOR = 0.005
# raw workbook Tech labels mapped to the cost-category names used downstream
TECH_TO_CATEGORY = {
    "hydro - Identified": "geohydro_discovered",
    "hydro - unidentified": "geohydro_undiscovered",
    "deep EGS": "egs_allkm",
    "NF - EGS": "egs_nearfield",
}
# workbook cost column dropped from the final ReEDS output (kWh-normalized duplicate)
DROPPED_COST_CAT = "Fixed O&M $/kWh"
CAPCOST_COL_RENAME = {"Cap cost $/kW": "Cap cost 1000$/MW"}
COMPARISON_METRIC = "Cap cost 1000$/MW"
# output data is in thousand $/MW; plots are shown in million $/MW
COMPARISON_PLOT_UNIT_SCALE = 1 / 1000
COMPARISON_PLOT_COL = "Cap cost (million $/MW)"


#%% ===========================================================================
### --- FUNCTIONS ---
### ===========================================================================
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--atb-file", required=True,
        help=(
            "Path to the ATB class-results workbook to use. If stored "
            "in this folder can just be the name of the file."
        )
    )
    parser.add_argument(
        "--reeds-path", required=True,
        help=(
            "Path to a local ReEDS repo clone. Used to load the hydrothermal "
            "resource/discovery-factor inputs (required) and the existing "
            "geo_ATB cost files for the comparison plot (optional)."
        ),
    )
    parser.add_argument(
        "--atb-year", type=int, required=True,
        help="ATB year used in output filenames and to locate baseline comparison files.",
    )
    parser.add_argument(
        "--base-atb-year", type=int,
        help=(
            "ATB files to read from ReEDS for comparison. If not supplied defaults to "
            "value of provided in --atb-year."
        )
    )
    parser.add_argument(
        "--copy-to-reeds", action="store_true",
        help="Copy new files to ReEDS inputs folder",
    )
    parser.add_argument(
        "--no-plot", action="store_true",
        help="Skip generating the baseline comparison plot.",
    )
    return parser.parse_args()


def load_raw_class_data(atb_file):
    """Read the Conservative/Moderate/Advanced sheets from the ATB workbook."""
    print(f"Loading raw ATB class data: {atb_file}")    
    sheets = {
        case: pd.read_excel(atb_file, sheet_name=case.capitalize())
        for case in ATB_SCENARIOS
    }
    return sheets["conservative"], sheets["moderate"], sheets["advanced"]


def build_case_scenarios(atb_cons, atb_mod, atb_adv):
    """Combine the three sheets into one table with a 'case' column and full
    Year coverage for the known workbook data points (start + 2035 target)."""
    # moderate/advanced decline from the conservative value to their own 2035 target
    atb_mod = pd.concat([atb_cons, atb_mod], ignore_index=True)
    atb_adv = pd.concat([atb_cons, atb_adv], ignore_index=True)

    # conservative persists flat, so duplicate it at the 2035 target year
    atb_cons_adj = atb_cons.copy()
    atb_cons_adj["Year"] = DECLINE_START_YEAR
    atb_cons = pd.concat([atb_cons, atb_cons_adj], ignore_index=True)

    atb_cons["case"] = "conservative"
    atb_mod["case"] = "moderate"
    atb_adv["case"] = "advanced"

    atb_out = pd.concat([atb_mod, atb_adv, atb_cons], ignore_index=True)
    atb_out["Tech"] = atb_out["Tech"].str.strip()

    # raw Depth is dropped here; ReEDS depth categories are derived later from Tech naming
    atb_out = atb_out.drop(columns=["Depth"], errors="ignore")
    return atb_out


def validate_class_coverage(atb_out):
    """Raise an error if any case/Tech is missing a Geo class present elsewhere
    in the workbook"""
    expected_classes = sorted(atb_out["Geo class"].dropna().unique())

    gaps = []
    for case, case_df in atb_out.groupby("case"):
        for tech, tech_df in case_df.groupby("Tech"):
            present = set(tech_df["Geo class"].dropna().unique())
            missing = sorted(set(expected_classes) - present)
            if missing:
                gaps.append((case, tech, missing))

    if gaps:
        detail = "\n".join(
            f"  case={case!r} Tech={tech!r} missing Geo class(es): {missing}"
            for case, tech, missing in gaps
        )
        raise ValueError(
            "Missing geothermal class data detected in the raw ATB workbook; "
            "this requires manual review before proceeding:\n" + detail
        )

    print("Class coverage check passed: all Tech/Geo class combinations present.")


def expand_full_year_range(atb_out):
    """Expand to every Tech x Geo class x case combination over YEAR_RANGE,
    and melt cost columns into a long 'cost_cat'/'value' format."""
    id_cols = ["case", "Tech", "Geo class"]
    combos = atb_out[id_cols].drop_duplicates()
    years = pd.DataFrame({"Year": list(YEAR_RANGE)})
    full_grid = combos.merge(years, how="cross")

    atb_full = full_grid.merge(atb_out, on=id_cols + ["Year"], how="left")

    value_cols = [c for c in atb_full.columns if c not in id_cols + ["Year"]]
    long_df = atb_full.melt(
        id_vars=id_cols + ["Year"], value_vars=value_cols,
        var_name="cost_cat", value_name="value",
    )
    return long_df


def interpolate_and_extrapolate(long_df):
    """Interpolate between known years, backfill before the earliest year,
    forward-fill after 2035, then apply the post-2035 annual cost decline."""
    group_cols = ["case", "Tech", "Geo class", "cost_cat"]
    long_df = long_df.sort_values(group_cols + ["Year"]).reset_index(drop=True)

    grouped_value = long_df.groupby(group_cols)["value"]
    long_df["value"] = grouped_value.transform(
        lambda s: s.interpolate(method="linear", limit_area="inside")
    )
    long_df["value"] = long_df.groupby(group_cols)["value"].transform(lambda s: s.bfill())
    long_df["value"] = long_df.groupby(group_cols)["value"].transform(lambda s: s.ffill())

    decline_mult = np.where(
        long_df["Year"] > DECLINE_START_YEAR,
        (1 - IMPROVEMENT_FACTOR) ** (long_df["Year"] - DECLINE_START_YEAR),
        1.0,
    )
    long_df["value"] = long_df["value"] * decline_mult
    return long_df


def rename_tech_categories(long_df):
    """Map raw workbook Tech labels to the cost-category names used downstream."""
    long_df = long_df.copy()
    long_df["category"] = long_df["Tech"].str.strip().map(TECH_TO_CATEGORY)

    unmapped = sorted(long_df.loc[long_df["category"].isna(), "Tech"].unique())
    if unmapped:
        raise ValueError(f"Unrecognized Tech values with no category mapping: {unmapped}")
    return long_df


def load_hydrothermal_weights(reeds_path):
    """Load hydrothermal resource capacity and discovery fraction from a ReEDS
    repo, returning per-class weights for the discovered/undiscovered split."""
    geo_dir = reeds_path / "inputs" / "geothermal"
    rsc_file = geo_dir / "geo_rsc_ATB_2023.csv"
    discovery_file = geo_dir / "geo_discovery_factor_ATB_2023.csv"
    for required_file in (rsc_file, discovery_file):
        if not required_file.is_file():
            raise FileNotFoundError(f"Required ReEDS input not found: {required_file}")

    print(f"Loading hydrothermal resource and discovery data from {geo_dir}")
    rsc = pd.read_csv(rsc_file)
    rsc = rsc.loc[rsc["sc_cat"] == "cap"].rename(columns={"value": "cap_MW"})

    discovery = pd.read_csv(discovery_file).rename(columns={"value": "frac_discovered"})

    # inner join: only geohydro resource rows have a matching discovery fraction
    geohydro = rsc.merge(discovery, on=["*i", "r"])
    geohydro["geohydro_discovered"] = geohydro["cap_MW"] * geohydro["frac_discovered"]
    geohydro["geohydro_undiscovered"] = geohydro["cap_MW"] * (1 - geohydro["frac_discovered"])
    geohydro["class"] = geohydro["*i"].str.rsplit("_", n=1).str[-1].astype(int)

    agg = geohydro.groupby("class", as_index=False)[
        ["geohydro_discovered", "geohydro_undiscovered"]
    ].sum()

    weights = agg.melt(id_vars="class", var_name="category", value_name="weight")
    weights = weights.rename(columns={"class": "Geo class"})
    return weights


def apply_hydrothermal_weighting(long_df, weights):
    """Combine discovered/undiscovered hydrothermal costs into a single
    capacity-weighted 'geohydro_allkm' category; pass other categories through."""
    merged = long_df.merge(weights, on=["category", "Geo class"], how="left")
    merged["weight"] = merged["weight"].fillna(1.0)
    merged["category"] = merged["category"].replace(
        {"geohydro_discovered": "geohydro_allkm", "geohydro_undiscovered": "geohydro_allkm"}
    )

    group_cols = ["case", "category", "Year", "Geo class", "cost_cat"]
    averaged = merged.groupby(group_cols).apply(
        lambda g: pd.Series({"value": np.average(g["value"], weights=g["weight"])})
    ).reset_index()

    averaged["value"] = averaged["value"].round(6)
    return averaged


def finalize_output_table(averaged):
    """Split category into Tech/Depth, pivot cost categories to columns, and
    format to match the ReEDS plant_characteristics geo_ATB file layout."""
    df = averaged.copy()
    df["Tech"] = df["category"].str.split("_", n=1).str[0]
    df["Depth"] = df["category"].str.split("_", n=1).str[1]

    pivot_input = df[df["cost_cat"] != DROPPED_COST_CAT]
    wide = pivot_input.pivot_table(
        index=["case", "Tech", "Geo class", "Depth", "Year"],
        columns="cost_cat", values="value",
    ).reset_index()
    wide.columns.name = None

    wide = wide.rename(columns=CAPCOST_COL_RENAME)
    wide["Var O&M $/MWh"] = 0

    wide = wide.sort_values(["case", "Year", "Tech", "Depth", "Geo class"]).reset_index(drop=True)
    return wide


def write_case_outputs(wide, output_dir, atb_year, copy_to_reeds, inputs_dir):
    """Write one CSV per case, matching the ReEDS plant_characteristics naming."""
    output_dir.mkdir(parents=True, exist_ok=True)

    case_outputs = {}
    for case, case_df in wide.groupby("case"):
        case_df = case_df.drop(columns=["case"])
        out_path = output_dir / f"geo_ATB_{atb_year}_{case}.csv"
        case_df.to_csv(out_path, index=False)
        print(f"Wrote {out_path}")
        case_outputs[case] = case_df
        if copy_to_reeds:
            reeds_out_path = inputs_dir / f"geo_ATB_{atb_year}_{case}.csv"
            shutil.copy2(out_path, reeds_out_path)
            print(f"...copied to {reeds_out_path}")

    return case_outputs


def load_comparison_baseline(inputs_dir, cases, atb_year):
    """Load existing geo_ATB cost files from a ReEDS repo to use as a baseline
    for the comparison plot. Returns None if no baseline files are found."""
    frames = []
    print(f"Loading baseline comparison files for {atb_year} from ReEDS")
    for case in cases:
        path = inputs_dir / f"geo_ATB_{atb_year}_{case}.csv"
        if not path.is_file():
            print(f"Warning: baseline file not found, skipping comparison for {case}: {path}")
            continue
        print(f"...loaded {path}")
        case_df = pd.read_csv(path)
        case_df["case"] = case
        frames.append(case_df)

    if not frames:
        return None

    baseline = pd.concat(frames, ignore_index=True)
    return baseline


def plot_comparison(updated, baseline, output_path, atb_year, base_atb_year, harmonize_yaxis=False):
    """Plot capital cost vs. Year, faceted by Tech+Depth (rows) and Geo class
    (columns), comparing the newly generated values to the baseline files.
    If harmonize_yaxis is True, every subplot shares one y-axis starting at 0."""
    updated = updated.copy()
    baseline = baseline.copy()
    if atb_year != base_atb_year:
        updated["source"] = f"ATB {atb_year}"
        baseline["source"] = f"ATB {base_atb_year}"
        source_styles = {f"ATB {atb_year}": "-", f"ATB {base_atb_year}": "--"}
    else:
        updated["source"] = "updated"
        baseline["source"] = "baseline"
        source_styles = {"updated": "-", "baseline": "--"}

    combined = pd.concat([updated, baseline], ignore_index=True)
    combined[COMPARISON_PLOT_COL] = combined[COMPARISON_METRIC] * COMPARISON_PLOT_UNIT_SCALE

    row_keys = sorted(combined[["Tech", "Depth"]].drop_duplicates().itertuples(index=False, name=None))
    col_keys = sorted(combined["Geo class"].unique())
    cases = sorted(combined["case"].unique())
    case_colors = dict(zip(cases, plt.cm.tab10.colors))

    fig, axes = plt.subplots(
        len(row_keys), len(col_keys),
        figsize=(3 * len(col_keys), 2.2 * len(row_keys)),
        sharex=True, sharey=harmonize_yaxis, squeeze=False,
    )

    legend_entries = {}
    for i, (tech, depth) in enumerate(row_keys):
        for j, geo_class in enumerate(col_keys):
            ax = axes[i][j]
            subset = combined[
                (combined["Tech"] == tech) & (combined["Depth"] == depth) & (combined["Geo class"] == geo_class)
            ]
            for case in cases:
                for source, style in source_styles.items():
                    series = subset[
                        (subset["case"] == case) & (subset["source"] == source)
                    ].sort_values("Year")
                    if series.empty:
                        continue
                    label = f"{case} ({source})"
                    line, = ax.plot(
                        series["Year"], series[COMPARISON_PLOT_COL],
                        linestyle=style, color=case_colors[case], label=label,
                    )
                    legend_entries.setdefault(label, line)

            if i == 0:
                ax.set_title(f"Class {geo_class}")
            if j == 0:
                ax.set_ylabel(f"{tech}\n{depth}")
            ax.set_xticks([2020, 2050])

    if harmonize_yaxis:
        # sharey only aligns the axes; the bottom still needs to be pinned to 0
        axes[0][0].set_ylim(bottom=0)

    if legend_entries:
        fig.legend(
            legend_entries.values(), legend_entries.keys(),
            loc="lower center", ncol=min(len(legend_entries), 4), bbox_to_anchor=(0.5, -0.02),
        )

    fig.suptitle(f"Geothermal {COMPARISON_PLOT_COL}: updated vs. baseline")
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote comparison plot: {output_path}")


def main():
    args = parse_args()
    reeds_path = Path(args.reeds_path)
    output_dir = THISDIR / "output"
    inputs_dir = reeds_path / "inputs" / "plant_characteristics"


    atb_cons, atb_mod, atb_adv = load_raw_class_data(args.atb_file)
    atb_out = build_case_scenarios(atb_cons, atb_mod, atb_adv)
    validate_class_coverage(atb_out)

    long_df = expand_full_year_range(atb_out)
    long_df = interpolate_and_extrapolate(long_df)
    long_df = rename_tech_categories(long_df)

    weights = load_hydrothermal_weights(reeds_path)
    averaged = apply_hydrothermal_weighting(long_df, weights)

    wide = finalize_output_table(averaged)
    case_outputs = write_case_outputs(wide, output_dir, args.atb_year, args.copy_to_reeds, inputs_dir)

    if not args.no_plot:
        if args.base_atb_year is None:
            args.base_atb_year = args.atb_year
        
        baseline = load_comparison_baseline(inputs_dir, sorted(case_outputs), args.base_atb_year)

        if baseline is not None:
            updated = pd.concat(
                [case_df.assign(case=case) for case, case_df in case_outputs.items()],
                ignore_index=True,
            )
            plot_comparison(
                updated, 
                baseline, 
                output_dir / "geo_atb_comparison.png", 
                args.atb_year, 
                args.base_atb_year
            )
            plot_comparison(
                updated,
                baseline,
                output_dir / "geo_atb_comparison_harmonized.png",
                args.atb_year,
                args.base_atb_year,
                harmonize_yaxis=True,
            )
        else:
            print("Skipping comparison plot: no baseline files found.")
    
    if args.copy_to_reeds:
        print(
            "\nFile copied to ReEDS. Don't forget to update cases.csv and "
            "inputs/plant_characteristics/dollaryear.csv\n"
            )


if __name__ == "__main__":
    main()
