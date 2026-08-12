# ATB historical baseline

These CSVs provide the ReEDS years that are not present in the current scraped
ATB release. They were initially seeded from
`C:/ReEDS/ReEDS/inputs/plant_characteristics/*_ATB_2024_*.csv` and are stored in
constant 2022 dollars, as configured by `historical_data.dollar_year`.

For each technology series and scenario, `generate_atb_files.py`:

1. reads these rows before the first year available in the current raw ATB;
2. concatenates the current scraped ATB projection from that year onward;
3. preserves complete historical series that are no longer published by ATB;
4. appends the first current projection year to this baseline if it is missing.

Step 4 makes the update idempotent and prepares continuity for the next annual
release. For example, ATB 2024 uses history through 2021 and projections from
2022. After the run, 2022 is stored here; if the next release begins in 2023,
the formatter uses history through 2022 and the new projection from 2023.

The formatter validates every technology/scenario series from
`processing.reeds_start_year` through its final year and stops with an error if
any year is missing.

Missing files may be initialized from the configured ReEDS repository when
`historical_data.seed_missing_from_reeds` is `true`. Once seeded, these files
are the versioned source of truth and should be committed with the pipeline.
