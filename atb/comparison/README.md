# Local ATB output comparison

The comparison utility is versioned. Generated/ReEDS validation plots remain
local and ignored, while before/after smoothing plots are versioned for review.

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

Only plots are written, under `comparison/plots/`:

- `comparison_overview.png` summarizes file-level validation status;
- one time-series plot is written for each generated/ReEDS file pair.

Detailed comparison statuses and reverse-coverage gaps are printed to the
terminal instead of being written as CSV reports. In these local plots, solid
lines are the unsmoothed generated baseline and dashed lines are ReEDS.

Running `python comparison/compare_atb_outputs.py` by itself compares the final
files already in `output/` with ReEDS. It cannot regenerate before/after plots
because the temporary pre-smoothing data is intentionally not retained.

The full pipeline also compares the temporary unsmoothed data with the final
smoothed outputs. Those plots are written to
`smoothing_comparison/` and are versioned so branch users can review
the smoothing effect without retaining duplicate CSV outputs. Final processed
values are lines, while solid dots identify input data values. Manual history
and directly observed real history receive dots; broadcast history and filled
real-history years do not. Future dots show the raw ATB values, while the line
shows the final raw or smoothed trajectory. A selected manual or broadcast
history uses one color for its entire historical curve. Real history uses green
for observations and orange for years filled from that same real series; an
internal gap is linearly interpolated. Gray and gold distinguish raw and
smoothed ATB projections. A dotted vertical line marks the configured
projection start year; line styles distinguish technology series. Each interval
uses the source color of its starting year, so a new source color never extends
backward into the preceding year.
