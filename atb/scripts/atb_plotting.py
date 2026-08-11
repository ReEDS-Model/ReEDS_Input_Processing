"""Plot raw NLR ATB metrics using the shared workflow configuration."""

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from atb_config import ATBE_COLUMN_MAPPING, load_config, raw_file_path, resolve_atb_path


SCENARIOS = ("Moderate", "Advanced", "Conservative")
UNITS = {
    "CAPEX": "USD/kW",
    "OCC": "USD/kW",
    "Fixed O&M": "USD/kW-yr",
    "Variable O&M": "USD/MWh",
}


def load_raw_atb(config):
    """Load the same local flat file used by the ReEDS formatter."""
    path = raw_file_path(config, "flat_file")
    if not path.is_file():
        raise FileNotFoundError(
            f"Raw ATB flat file not found: {path}\n"
            "Run 'python scrape_atb_inputs.py --only flat' first."
        )
    print(f"Loading raw ATB flat file: {path}")
    return pd.read_csv(path, low_memory=False).rename(columns=ATBE_COLUMN_MAPPING)


def clean_metric_data(raw, metric, case, crp_years, technologies):
    """Select and aggregate one metric into a plotting-friendly table."""
    required = {
        "Technology", "DisplayName", "Scenario", "Parameter", "Case",
        "CRPYears", "variable", "value",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"Raw ATB flat file is missing columns: {missing}")

    data = raw.loc[
        (raw["Parameter"] == metric)
        & (raw["Case"] == case)
        & (raw["CRPYears"] == crp_years)
        & (raw["Technology"].isin(technologies))
        & (raw["Scenario"].isin(SCENARIOS)),
        ["Technology", "DisplayName", "Scenario", "variable", "value"],
    ].copy()
    data["variable"] = pd.to_numeric(data["variable"], errors="coerce")
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data = data.dropna(subset=["variable", "value"])
    # Some raw ATBe rows differ in metadata not needed for plotting. Average
    # exact display/scenario/year duplicates into one visible series.
    return (
        data.groupby(
            ["Technology", "DisplayName", "Scenario", "variable"],
            as_index=False,
        )["value"]
        .mean()
        .sort_values(["Technology", "DisplayName", "Scenario", "variable"])
    )


def plot_metric(data, metric, config, figure_format, output_dir):
    """Write one multi-panel figure and return its path."""
    technologies = config["plotting"]["technologies"]
    technologies = [tech for tech in technologies if tech in set(data["Technology"])]
    if not technologies:
        raise ValueError(
            f"No data found for metric={metric!r}, case={config['plotting']['case']!r}, "
            f"CRPYears={config['plotting']['crp_years']}."
        )

    ncols = min(3, len(technologies))
    nrows = math.ceil(len(technologies) / ncols)
    figure, axes = plt.subplots(
        nrows, ncols, figsize=(5.2 * ncols, 3.6 * nrows), squeeze=False
    )
    colors = plt.get_cmap("tab20")

    for panel, technology in enumerate(technologies):
        axis = axes.flat[panel]
        tech_data = data.loc[data["Technology"] == technology]
        display_names = sorted(tech_data["DisplayName"].dropna().unique())
        for index, display_name in enumerate(display_names):
            series = tech_data.loc[tech_data["DisplayName"] == display_name]
            pivot = series.pivot_table(
                index="variable", columns="Scenario", values="value", aggfunc="mean"
            ).sort_index()
            if "Moderate" not in pivot:
                continue
            color = colors(index % colors.N)
            axis.plot(pivot.index, pivot["Moderate"], color=color, label=display_name)
            if {"Advanced", "Conservative"}.issubset(pivot.columns):
                band = pivot[["Advanced", "Conservative"]].dropna()
                axis.fill_between(
                    band.index,
                    band.min(axis=1),
                    band.max(axis=1),
                    color=color,
                    alpha=0.18,
                    linewidth=0,
                )
        axis.set_title(technology)
        axis.set_xlabel("Projection year")
        unit = UNITS.get(metric, "value")
        axis.set_ylabel(f"{metric} ({config['atb']['dollar_year']} {unit})")
        axis.grid(axis="y", linestyle=":", alpha=0.5)
        if len(display_names) > 1:
            axis.legend(fontsize=7, frameon=False)

    for panel in range(len(technologies), nrows * ncols):
        axes.flat[panel].set_visible(False)

    figure.suptitle(
        f"NLR ATB {config['atb']['year']} — {metric} "
        f"({config['plotting']['case']}, CRP {config['plotting']['crp_years']})"
    )
    figure.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_metric = metric.lower().replace(" ", "_").replace("&", "and")
    output = output_dir / f"atb_{config['atb']['year']}_{safe_metric}.{figure_format}"
    figure.savefig(output, dpi=200, bbox_inches="tight")
    plt.close(figure)
    print(f"Saved plot: {output}")
    return output


def make_plots(config, metrics=None, show=False, save_cleaned=False):
    """Create all configured plots from the local raw flat file."""
    plot_config = config["plotting"]
    metrics = metrics or plot_config["metrics"]
    raw = load_raw_atb(config)
    output_dir = resolve_atb_path(plot_config["output_directory"])
    outputs = []
    for metric in metrics:
        data = clean_metric_data(
            raw,
            metric,
            plot_config["case"],
            plot_config["crp_years"],
            plot_config["technologies"],
        )
        if save_cleaned:
            output_dir.mkdir(parents=True, exist_ok=True)
            clean_path = output_dir / (
                f"atb_{config['atb']['year']}_{metric.lower().replace(' ', '_')}_cleaned.csv"
            )
            data.to_csv(clean_path, index=False)
            print(f"Saved cleaned plotting data: {clean_path}")
        outputs.append(
            plot_metric(data, metric, config, plot_config["figure_format"], output_dir)
        )
    if show:
        print("Plots were saved; open the files listed above to view them.")
    return outputs


def main():
    parser = argparse.ArgumentParser(description="Plot configured raw NLR ATB metrics.")
    parser.add_argument("--config", help="Path to config.yaml (default: ../config.yaml).")
    parser.add_argument(
        "--metric", "-m", nargs="+",
        help="One or more ATB parameters; defaults to plotting.metrics in config.yaml.",
    )
    parser.add_argument(
        "--save-cleaned", action="store_true",
        help="Also save the cleaned data behind each plot.",
    )
    parser.add_argument("--show", action="store_true", help="Print a viewing reminder.")
    args = parser.parse_args()
    make_plots(
        load_config(args.config),
        metrics=args.metric,
        show=args.show,
        save_cleaned=args.save_cleaned,
    )


if __name__ == "__main__":
    main()
