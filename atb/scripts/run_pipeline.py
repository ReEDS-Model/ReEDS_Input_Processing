"""Run the config-selected NLR ATB workflow stages in order."""

import argparse
import subprocess
import sys
from pathlib import Path

from atb_config import DEFAULT_CONFIG_PATH, load_config


SCRIPT_DIR = Path(__file__).resolve().parent
STAGES = {
    "scrape": ("scrape_raw_data", "scrape_atb_inputs.py"),
    "format": ("format_reeds_inputs", "generate_atb_files.py"),
    "plot": ("make_plots", "atb_plotting.py"),
}


def run_stage(name, script, config_path, force_download=False):
    command = [sys.executable, str(SCRIPT_DIR / script), "--config", str(config_path)]
    if name == "scrape" and force_download:
        command.append("--force")
    print(f"\n=== {name.upper()} ===", flush=True)
    subprocess.run(command, check=True)


def main():
    parser = argparse.ArgumentParser(
        description="Run scrape, ReEDS formatting, and plotting from config.yaml."
    )
    parser.add_argument("--config", help="Path to config.yaml (default: ../config.yaml).")
    parser.add_argument(
        "--only", nargs="+", choices=list(STAGES),
        help="Run only the selected stage(s), ignoring workflow switches.",
    )
    parser.add_argument(
        "--force-download", action="store_true",
        help="Replace existing raw files during the scrape stage.",
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve() if args.config else DEFAULT_CONFIG_PATH
    config = load_config(config_path)
    selected = set(args.only) if args.only else None

    plan = []
    for stage, (switch, script) in STAGES.items():
        enabled = stage in selected if selected is not None else config["workflow"][switch]
        plan.append((stage, script, enabled))

    print(f"Config: {config_path}")
    print(f"ATB year: {config['atb']['year']}")
    print("Workflow:")
    for stage, _, enabled in plan:
        print(f"  {'RUN ' if enabled else 'SKIP'} {stage}")

    for stage, script, enabled in plan:
        if enabled:
            run_stage(stage, script, config_path, args.force_download)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
