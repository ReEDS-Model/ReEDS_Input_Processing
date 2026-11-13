# ATB output comparison

Checks the generated files against the ReEDS repository set in
`../config.yaml` and plots the result. The script is versioned; its plots are
git-ignored. Run from `atb/`:

```bash
python scripts/run_pipeline.py --only format compare
```

The pipeline keeps a temporary copy of the files just before smoothing and
compares that copy with ReEDS, so smoothing does not hide formatting
differences. It checks missing files and columns, duplicate keys, rows on one
side only, and values outside the numeric tolerances. Statuses are printed to
the terminal.

## Plots

- `plot_comparison/comparison_overview.png`: status per file.
- `plot_comparison/`: one figure per technology, generated vs. ReEDS. Color
  is the scenario and line style the sub-technology. The thin line is
  generated and the thick translucent line is ReEDS. Each side keeps its own
  dollar year, named in the title and on the axis. ReEDS files are the ATB
  vintage in `processing.reeds_atb_year`; beccs, which has no ATB file in
  ReEDS, is compared with `beccs_reference.csv`.
- `plot_comparison/ccs_retrofit_ATB_<year>.png`: the CCS retrofit multipliers
  and costs against the files in ReEDS `inputs/upgrades/`. It needs
  `scripts/ccs_retrofit_inputs.py` to have run.
- `plot_component/`: one figure per technology, before vs. after smoothing.
  Lines are final values and dots are input values, colored by source:

| Color | Source |
| --- | --- |
| green | observed (`real`, `indexed`) |
| orange | filled between observations |
| blue | ReEDS baseline (`inputs/historical/reeds_baseline.csv`) |
| pink | backfill |
| gray | unavailable placeholder |
| purple / dark gray / teal | raw ATB conservative / moderate / advanced |
| gold | smoothed ATB projection |

A dotted vertical line marks each series' ATB start year.

`python comparison/compare_atb_outputs.py` alone compares the files already
in `output/` with ReEDS; it cannot make the before/after plots.
