# Local ATB output comparison

The comparison utility is versioned; the plots it generates are not. Both plot
directories are ignored by Git, so rerun the pipeline to regenerate them.

For validation plus before/after smoothing figures, run formatting and
comparison together from `atb/`:

```bash
python scripts/run_pipeline.py --only format compare
```

When the full pipeline runs formatting and comparison together, it captures
fully processed files just before smoothing in a temporary directory. The
comparison script validates those files against the matching source files in
the ReEDS repository configured in `../config.yaml`, then the temporary files
are deleted automatically. Thus smoothing differences do not obscure scraper
and formatting validation. The script normalizes legacy wind filenames and
column names, then checks:

- missing files and columns;
- duplicate keys;
- rows present on only one side;
- changed values using configurable numeric tolerances.

Only plots are written, under `comparison/plot_comparison/`:

- `comparison_overview.png` summarizes file-level validation status;
- one time-series plot is written for each generated/ReEDS file pair.

Detailed comparison statuses and reverse-coverage gaps are printed to the
terminal instead of being written as CSV reports. In these local plots, solid
lines are the unsmoothed generated baseline and dashed lines are ReEDS.

Running `python comparison/compare_atb_outputs.py` by itself compares the final
files already in `output/` with ReEDS. It cannot regenerate before/after plots
because the temporary pre-smoothing data is intentionally not retained.

The full pipeline also compares the temporary unsmoothed data with the final
smoothed outputs. Those plots are written to `plot_component/`, which lets
reviewers see the smoothing effect without retaining duplicate CSV outputs. Final processed
values are lines, while solid dots identify input data values. Manual history
and directly observed real history receive dots; broadcast history and filled
real-history years do not. Future dots show the raw ATB values, while the line
shows the final raw or smoothed trajectory. A selected manual or broadcast
history uses one color for its entire historical curve. Real history uses green
for observations and orange for years filled from that same real series; an
internal gap is linearly interpolated. Gray and gold distinguish raw and
smoothed ATB projections. A dotted vertical line marks the configured
projection start year; line styles distinguish technology series. Each interval
takes the color of the year it ends in, so a new source color appears on the
interval that reaches its first year. The one exception is the interval leaving
the last historical year: it keeps the historical color, so the step into the
first projection year still reads as history.
