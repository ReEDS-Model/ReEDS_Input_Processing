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
- one time-series plot is written per technology, with every scenario
  (conservative, moderate, advanced) drawn in the same panels.

Detailed comparison statuses and reverse-coverage gaps are printed to the
terminal instead of being written as CSV reports. In these local plots, color
identifies the scenario and line style identifies the sub-technology series
(turbine classes, CSP types, plant types). The thin crisp line is the
unsmoothed generated baseline and the thick translucent line underneath is
ReEDS, so a match shows as a thin line centred in a halo.

Running `python comparison/compare_atb_outputs.py` by itself compares the final
files already in `output/` with ReEDS. It cannot regenerate before/after plots
because the temporary pre-smoothing data is intentionally not retained.

The full pipeline also compares the temporary unsmoothed data with the final
smoothed outputs. Those plots are written to `plot_component/`, which lets
reviewers see the smoothing effect without retaining duplicate CSV outputs.
These plots use the same one-figure-per-technology layout, with line style
identifying the sub-technology series. History is shared across scenarios, so
only the projections fan out. Each scenario's raw ATB projection has its own
color (purple conservative, dark gray moderate, teal advanced) and marker
shape (down-triangle, square, up-triangle); historical input values stay round
dots in their provenance color. Final processed values are lines, while
markers identify input data values. Observed real
anchors, indexed O&M anchors, and manual history receive dots; broadcast,
placeholder, and filled years do not. Future markers show the raw ATB values;
where a real observation replaces ATB inside the overlap window, only the real
marker is drawn. Real history uses green for observations
and orange for filled years; indexed history is green with dark anchors; the
unavailable placeholder is light gray. Smoothed ATB projections are gold whatever
the scenario. A dotted vertical line marks each series' ATB start
year (two lines where sub-technologies start in different years). Each interval
takes the color of the year it ends in, so a new source color appears on the
interval that reaches its first year, and the step from the last historical
year into the first projection year is drawn in the projection color.
