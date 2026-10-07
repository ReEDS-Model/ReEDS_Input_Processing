# Geothermal ATB cost-by-class processing

The published NREL Annual Technology Baseline (ATB) does not break out
geothermal costs by resource class, so geothermal is handled by this separate
workflow rather than the main [`atb/`](../README.md) pipeline.

[`geo_atb.py`](geo_atb.py) builds the ReEDS `geo_ATB_<year>_<case>.csv` cost
inputs from a class-results workbook, and optionally compares the result
against an existing ReEDS repo checkout.


## Processing assumptions

1. Conservative-case values persist through 2035.
2. Moderate and advanced cases start at the conservative value and decline to
   a 2035 target value taken from the workbook.
3. Beyond 2035, all cases decline by a fixed 0.5% annually.
4. Hydrothermal discovered/undiscovered costs are combined into a single
   `geohydro_allkm` category using a capacity-weighted average, based on
   resource and discovery-fraction data from a ReEDS repo.

If the workbook is missing a Tech/Geo class combination
that exists elsewhere in the data, the script raises an error listing the
gaps so they can be reviewed manually.

## Inputs

- `ATB-REeDS-2025-Class-Results.xlsx` (`--atb-file`): raw class-level
  cost results, with `Conservative`, `Moderate`, and `Advanced` sheets.
  Supplied by Erik Witter and Dayo Akindipe from the geothermal team.
- Path to a local ReEDS repo (`--reeds-path`).
  - Used to read `inputs/geothermal/geo_rsc_ATB_2023.csv` and
    `inputs/geothermal/geo_discovery_factor_ATB_2023.csv` for the
    hydrothermal capacity-weighted averaging step.
  - Used to read `inputs/plant_characteristics/geo_ATB_<year>_<case>.csv` 
    baseline files for the comparison plot.
  - Location to save new geothermal outputs


## Run

Run in the `reeds` conda environment:

```bash
conda activate reeds
python geo_atb.py --atb-file filename.xlsx --reeds-path /path/to/ReEDS --atb_year int
```

Required arguments:
- `--atb-file`: path to the class-results workbook (defaults to the `.xlsx`
  in this directory).
- `--atb-year`: ATB year used in output filenames.

Optional arguments:
- `--base-atb-year`: ATB year for baseline files read from ReEDS. If unspecified defaults
  to value for `--atb-year`.
- `--copy-to-reeds`: copy new files to ReEDS inputs folder.
- `--no-plot`: skip the baseline comparison plot.

## Outputs

- `geo_ATB_<year>_conservative.csv`, `geo_ATB_<year>_moderate.csv`,
  `geo_ATB_<year>_advanced.csv`: final cost-by-class tables, formatted to
  match the ReEDS `inputs/plant_characteristics/` layout.
- `geo_atb_comparison.png`: capital cost vs. year, faceted by Tech/Depth and
  Geo class, comparing the new output to the baseline files from
  `--reeds-path` and `--base-atb-year` 
  (skipped if no baseline files are found, or if `--no-plot`is set).
- `geo_atb_comparison.png`: same as previous plot but with a harmonized y-axis 
  starting at zero.