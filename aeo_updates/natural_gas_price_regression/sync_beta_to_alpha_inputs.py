"""
Copy beta regression outputs into alpha regression inputs.

This removes the manual handoff step between:
1) aeo_beta_regression.py
2) aeo_alpha_regression.py

Copied files:
- outputs of beta regression/cd_beta0.csv -> inputs for alpha regression/cd_beta0.csv
- outputs of beta regression/national_beta.csv -> inputs for alpha regression/national_beta.csv
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from aeo_functions import (
    load_config,
    require,
    resolve_config_path,
    resolve_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy beta outputs into alpha input directory.")
    parser.add_argument("--config", default="aeo_pipeline_config.json")
    parser.add_argument("--beta-output-dir", default="outputs of beta regression")
    parser.add_argument(
        "--alpha-input-dir",
        default=None,
        help="Override alpha input directory (default: config paths.input_dir).",
    )
    return parser.parse_args()


def copy_required_files(beta_out_dir: Path, alpha_input_dir: Path) -> None:
    """Copy cd_beta0.csv and national_beta.csv from beta outputs to alpha inputs."""
    copies = [
        ("cd_beta0.csv", "cd_beta0.csv"),
        ("national_beta.csv", "national_beta.csv"),
    ]
    alpha_input_dir.mkdir(parents=True, exist_ok=True)

    for src_name, dst_name in copies:
        src = beta_out_dir / src_name
        dst = alpha_input_dir / dst_name
        require(src.exists(), f"Missing beta output file: {src}")
        shutil.copy2(src, dst)
        print(f"Copied: {src} -> {dst}")


def main() -> int:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    cfg_path = resolve_config_path(args.config, script_dir)

    config = load_config(cfg_path)
    base_dir = cfg_path.parent

    input_dir_cfg = str(config.get("paths", {}).get("input_dir", "inputs for alpha regression"))
    alpha_input_dir = resolve_path(base_dir, args.alpha_input_dir or input_dir_cfg)
    beta_out_dir = resolve_path(base_dir, args.beta_output_dir)
    require(beta_out_dir.exists(), f"Beta output directory not found: {beta_out_dir}")

    copy_required_files(beta_out_dir, alpha_input_dir)
    print("Beta-to-alpha input sync completed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
