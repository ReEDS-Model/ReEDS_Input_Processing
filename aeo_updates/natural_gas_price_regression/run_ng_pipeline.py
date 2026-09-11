#!/usr/bin/env python3
"""
Cross-platform runner for the natural gas price regression pipeline.

Usage:
    python run_ng_pipeline.py [--config CONFIG_FILE]

Replaces run_ng_pipeline.bat for compatibility with macOS / Linux.
"""

import argparse
import subprocess
import sys
from pathlib import Path


STEPS = [
    ("1/4", "Running beta regression...",            "aeo_beta_regression.py"),
    ("2/4", "Syncing beta outputs to alpha inputs...", "sync_beta_to_alpha_inputs.py"),
    ("3/4", "Running alpha regression...",           "aeo_alpha_regression.py"),
    ("4/4", "Generating visualization and validation...", "visualization.py"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the NG price regression pipeline.")
    parser.add_argument("--config", default="aeo_pipeline_config.json",
                        help="Path to the pipeline config JSON (default: aeo_pipeline_config.json)")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent

    for tag, msg, script in STEPS:
        print(f"[{tag}] {msg}")
        result = subprocess.run(
            [sys.executable, script, "--config", args.config],
            cwd=script_dir,
        )
        if result.returncode != 0:
            print(f"\nNG pipeline failed at step {tag} with exit code {result.returncode}.")
            return result.returncode

    print("\nNG pipeline finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
