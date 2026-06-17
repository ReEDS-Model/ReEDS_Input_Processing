# Generate comparison plots between the previous ReEDS state-policy inputs
# (derived from the June 2025 LBNL data, stored in `old ReEDS input/`) and the
# updated inputs (derived from the June 2026 LBNL data, stored in `outputs/`).
#
# Plots are saved as PNG files in this folder so they can be attached to a PR.

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

HERE          = os.path.dirname(os.path.abspath(__file__))
OLD_DIR       = os.path.join(HERE, "old ReEDS input")
NEW_DIR       = os.path.join(HERE, "..", "outputs")
INTERMED_DIR  = os.path.join(NEW_DIR, "intermediate outputs")
PLOT_DIR      = os.path.join(HERE, "plots")
os.makedirs(PLOT_DIR, exist_ok=True)

OLD_LABEL  = "Old (from June 2025 LBNL data)"
NEW_LABEL  = "New (from June 2026 LBNL data, piecewise interpolated)"
INTM_LABEL = "New (from June 2026 LBNL data, not interpolated)"


def plot_rps(value_col, title, fname):
    df_old = pd.read_csv(os.path.join(OLD_DIR, "rps_fraction0.csv"))
    df_new = pd.read_csv(os.path.join(NEW_DIR, "rps_fraction.csv"))
    df_int = pd.read_csv(os.path.join(INTERMED_DIR, "rps_fraction_intermediate.csv"))

    states = sorted(set(df_old["st"]).union(df_new["st"]))
    cols = 3
    rows = -(-len(states) // cols)

    fig, axes = plt.subplots(rows, cols, figsize=(20, 3.5 * rows), sharex=True, sharey=True)
    axes = axes.flatten()

    for i, state in enumerate(states):
        ax = axes[i]
        s_old = df_old[df_old["st"] == state]
        s_new = df_new[df_new["st"] == state]
        s_int = df_int[df_int["st"] == state]
        ax.plot(s_old["t"], s_old[value_col], label=OLD_LABEL, marker="s", markersize=7, color="C0")
        ax.plot(s_new["t"], s_new[value_col], label=NEW_LABEL, color="C3", linestyle="-", linewidth=3)
        ax.plot(s_int["t"], s_int[value_col], label=INTM_LABEL, marker="o", markersize=5, linestyle="None", color="C3")
        ax.set_title(state)
        ax.tick_params(labelbottom=True)
        ax.grid(True)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 0.995))
    fig.suptitle(title, fontsize=18, y=1.0)
    plt.tight_layout(rect=[0.03, 0.03, 1, 0.985])
    out_path = os.path.join(PLOT_DIR, fname)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {out_path}")


def plot_ces():
    df_old = pd.read_csv(os.path.join(OLD_DIR, "ces_fraction0.csv"))
    df_new = pd.read_csv(os.path.join(NEW_DIR, "ces_fraction.csv"))
    df_int = pd.read_csv(os.path.join(INTERMED_DIR, "ces_fraction_intermediate.csv"))

    df_old = df_old.rename(columns={"*t": "t"})
    df_new = df_new.rename(columns={"*t": "t"})
    df_int = df_int.rename(columns={"*t": "t"})

    states = sorted(set(df_old["st"]).union(df_new["st"]))
    cols = 3
    rows = -(-len(states) // cols)

    fig, axes = plt.subplots(rows, cols, figsize=(20, 3.5 * rows), sharex=True, sharey=True)
    axes = axes.flatten()

    for i, state in enumerate(states):
        ax = axes[i]
        s_old = df_old[df_old["st"] == state]
        s_new = df_new[df_new["st"] == state]
        s_int = df_int[df_int["st"] == state]
        ax.plot(s_old["t"], s_old["Value"], label=OLD_LABEL, marker="s", markersize=7, color="C0")
        ax.plot(s_new["t"], s_new["Value"], label=NEW_LABEL, color="C3", linestyle="-", linewidth=3)
        ax.plot(s_int["t"], s_int["Value"], label=INTM_LABEL, marker="o", markersize=5, linestyle="None", color="C3")
        ax.set_title(state)
        ax.tick_params(labelbottom=True)
        ax.grid(True)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 0.995))
    fig.suptitle("State CES Fraction Comparison", fontsize=18, y=1.0)
    plt.tight_layout(rect=[0.03, 0.03, 1, 0.985])
    out_path = os.path.join(PLOT_DIR, "ces_fraction_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {out_path}")


def plot_hydrofrac():
    df_old = pd.read_csv(os.path.join(OLD_DIR, "hydrofrac_policy0.csv"))
    df_new = pd.read_csv(os.path.join(NEW_DIR, "hydrofrac_policy.csv"))

    df_old = df_old.sort_values("st")
    df_new = df_new.sort_values("st")
    df_merged = pd.merge(df_old, df_new, on="st", how="inner", suffixes=("_old", "_new"))

    x = np.arange(len(df_merged["st"])) * 1.5
    width = 0.6

    # RPS_All bar plot
    fig, ax = plt.subplots(figsize=(18, 6))
    ax.bar(x - width / 2, df_merged["RPS_All_old"], width=width, label=f"RPS_All - {OLD_LABEL}",
           color="C0", edgecolor="black", linewidth=0.6, zorder=3)
    ax.bar(x + width / 2, df_merged["RPS_All_new"], width=width, label=f"RPS_All - {NEW_LABEL.split(',')[0]}",
           color="C3", edgecolor="black", linewidth=0.6, zorder=3)
    ax.set_title("Hydrofrac Policy - RPS_All Comparison")
    ax.set_xlabel("State")
    ax.set_ylabel("Fraction")
    ax.set_xticks(x)
    ax.set_xticklabels(df_merged["st"], rotation=90)
    ax.legend()
    ax.grid(True, axis="y", zorder=0)
    plt.tight_layout()
    out_path = os.path.join(PLOT_DIR, "hydrofrac_RPS_All_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {out_path}")

    # CES bar plot
    fig, ax = plt.subplots(figsize=(18, 6))
    ax.bar(x - width / 2, df_merged["CES_old"], width=width, label=f"CES - {OLD_LABEL}",
           color="C0", edgecolor="black", linewidth=0.6, zorder=3)
    ax.bar(x + width / 2, df_merged["CES_new"], width=width, label=f"CES - {NEW_LABEL.split(',')[0]}",
           color="C3", edgecolor="black", linewidth=0.6, zorder=3)
    ax.set_title("Hydrofrac Policy - CES Comparison")
    ax.set_xlabel("State")
    ax.set_ylabel("Fraction")
    ax.set_xticks(x)
    ax.set_xticklabels(df_merged["st"], rotation=90)
    ax.legend()
    ax.grid(True, axis="y", zorder=0)
    plt.tight_layout()
    out_path = os.path.join(PLOT_DIR, "hydrofrac_CES_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {out_path}")


if __name__ == "__main__":
    print("Generating comparison plots...")
    plot_rps("rps_all",   "State RPS All Fraction Comparison",   "rps_all_comparison.png")
    plot_rps("rps_solar", "State RPS Solar Fraction Comparison", "rps_solar_comparison.png")
    plot_rps("rps_wind",  "State RPS Wind Fraction Comparison",  "rps_wind_comparison.png")
    plot_ces()
    plot_hydrofrac()
    print("Done.")
