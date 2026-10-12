"""Process ba-level ReEDS wholesale electricity prices for the dGen team.
Note
"""
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

## user settings

# ReEDS repo containing the bokeh dgen_report (source of report.xlsx)
reedspath = "/projects/stdscen/stdscens_fy25/ReEDS-2.0"
# ReEDS repo containing hierarchy_agg131.csv for disaggregating regions
reedspath_local = "/projects/stdscen/bsergi/ReEDS-2.0"

output_dir = Path(__file__).parent
plots_dir = output_dir / "plots"
# TODO: make this a command line argument
output_csv = output_dir / "std_scen_25_dgen_prices.csv"

start_year = 2026
# res_marg price spikes above this threshold ($/MWh) are smoothed out
price_threshold = 100


def load_and_process_dgen_prices(reedspath, reedspath_local, start_year=2026, price_threshold=100):
    """Load ba-level electricity prices, smooth res_marg spikes, and disaggregate to ba.

    Returns (prices_smoothed, prices_final):
      - prices_smoothed: per (scenario, rb, year, type) smoothed price, pre-aggregation
      - prices_final: per (scenario, rb, year) price_usd_2024_per_MWh, fully processed
    """
    print("processing ba-level prices for dgen")

    # need Wholesale Electricity Price (2022$/MWh); rerun Bokeh to get ba-level results
    report_path = Path(reedspath) / "postprocessing" / "bokehpivot" / "out" / "dgen_report" / "report.xlsx"
    prices = pd.read_excel(report_path, sheet_name="elec_price")

    ## drop historical years
    prices_out = prices[prices["year"] >= start_year].copy()

    ## smooth price spikes (> 100 $/MWh) in early years
    res_marg_mask = (prices_out["type"] == "res_marg") & (prices_out["$"] <= price_threshold)
    res_marg_avg = (
        prices_out[res_marg_mask]
        .groupby(["scenario", "rb"])["$"]
        .mean()
        .rename("res_marg_avg")
        .reset_index()
    )
    prices_out = prices_out.merge(res_marg_avg, how="left", on=["scenario", "rb"])

    is_res_marg_spike = (prices_out["$"] > price_threshold) & (prices_out["type"] == "res_marg")
    prices_out["price"] = np.where(is_res_marg_spike, prices_out["res_marg_avg"], prices_out["$"])

    # report on share of entries smoothed, price types, and years
    # typically the smoothing is only needed in near-term years for res_marg
    updated = prices_out[prices_out["price"] != prices_out["$"]]
    print(
        f"{len(updated) / len(prices_out) * 100:.2f}% of {updated["type"].unique()} prices exceeded\n"
        f"threshold of {price_threshold} and were smoothed using the average of all regions.\n"
        f"Years with smoothing: {sorted(updated["year"].unique())}."
    )

    ## summarize by category
    prices_smoothed = prices_out[["scenario", "rb", "year", "type", "price"]]
    prices_out = (
        prices_out.groupby(["scenario", "rb", "year"])["price"]
        .sum()
        .rename("price_usd_2024_per_MWh")
        .reset_index()
    )

    ## convert aggregated regions back to disaggregated
    breakpoint()
    hierarchy_raw = pd.read_csv(
        Path(reedspath_local) / "inputs" / "hierarchy_agg131.csv", usecols=["ba", "aggreg"]
    )
    hierarchy_raw = hierarchy_raw.rename(columns={"ba": "rb"})
    prices_out = prices_out.rename(columns={"rb": "aggreg"})
    prices_final = prices_out.merge(hierarchy_raw, how="outer", on="aggreg")

    print(f"processed {prices_final['scenario'].nunique()} scenarios:")
    print(prices_final["scenario"].unique())

    return prices_smoothed, prices_final


def plot_dgen_prices(prices_smoothed, prices_final, plots_dir):
    """Save diagnostic and final plots of the processed dgen prices."""
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    # plot for visual inspection: smoothed price by type, one subplot per type
    types = sorted(prices_smoothed["type"].unique())
    fig, axes = plt.subplots(1, len(types), figsize=(10, 4), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, t in zip(axes, types):
        df_t = prices_smoothed[prices_smoothed["type"] == t]
        for _, group in df_t.groupby(["scenario", "rb"]):
            ax.plot(group["year"], group["price"])
        ax.set_title(t)
        ax.set_ylim(0, 600)
        ax.set_xlabel("year")
    axes[0].set_ylabel("$")
    fig.tight_layout()
    fig.savefig(plots_dir / "prices.png")
    plt.close(fig)

    # final look: price by rb, one subplot per scenario
    scenarios = sorted(prices_final["scenario"].unique())
    ncols = min(len(scenarios), 4)
    nrows = math.ceil(len(scenarios) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(10, 4 * nrows), sharey=True)
    axes = np.atleast_1d(axes).flatten()
    for ax, scenario in zip(axes, scenarios):
        df_s = prices_final[prices_final["scenario"] == scenario]
        for _, group in df_s.groupby("rb"):
            ax.plot(group["year"], group["price_usd_2024_per_MWh"], alpha=0.1, color="black")
        ax.set_title(scenario)
        ax.set_ylim(0, 600)
        ax.set_xlabel("year")
    for ax in axes[len(scenarios):]:
        ax.set_visible(False)
    fig.tight_layout()
    fig.savefig(plots_dir / "prices_final.png")
    plt.close(fig)

    # boxplot of 2050 prices by scenario
    prices_2050 = prices_final[prices_final["year"] == 2050]
    fig, ax = plt.subplots(figsize=(10, 4))
    data_by_scenario = [
        prices_2050.loc[prices_2050["scenario"] == scenario, "price_usd_2024_per_MWh"]
        for scenario in scenarios
    ]
    ax.boxplot(data_by_scenario, tick_labels=scenarios)
    rng = np.random.default_rng()
    for i, values in enumerate(data_by_scenario, start=1):
        jitter = rng.uniform(-0.1, 0.1, size=len(values))
        ax.scatter(np.full(len(values), i) + jitter, values, alpha=0.1, color="black")
    ax.set_ylabel("price_usd_2024_per_MWh")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(plots_dir / "prices_final_2050.png")
    plt.close(fig)


if __name__ == "__main__":
    prices_smoothed, prices_final = load_and_process_dgen_prices(
        reedspath, reedspath_local, start_year=start_year, price_threshold=price_threshold
    )
    plot_dgen_prices(prices_smoothed, prices_final, plots_dir)

    prices_final[["scenario", "rb", "year", "price_usd_2024_per_MWh"]].to_csv(output_csv, index=False)
    print(f"prices saved to {output_csv}")
