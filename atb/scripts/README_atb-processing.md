# ReEDS formatting stage

`generate_atb_files.py` converts already-downloaded raw ATB inputs into
per-technology ReEDS CSVs. Network access and raw-data acquisition belong to
`scrape_atb_inputs.py`, not to this stage.

## Inputs

- user choices from `../config.yaml`;
- technology mappings from `settings.yaml`;
- `../scraped_input/atb_<year>_flat_file.csv` as the primary ATB dataset;
- `../scraped_input/atb_<year>_workbook.xlsx` for the battery power/energy cost
  split;
- `../manual_input/` for CSP ratios, historic capacity factors, and a
  pre-release battery fallback;
- prior ReEDS plant-characteristic and financial files for history, deflators,
  and dollar-year metadata.

## Processing

For every selected technology, the formatter:

1. selects `Technology`, `DisplayName`, `Case`, and `CRPYears` rows;
2. pivots ATB parameters into ReEDS columns such as `capcost`, `fom`, `vom`,
   `heatrate`, `rte`, and `cf_improvement`;
3. applies technology-specific battery, CSP, coal, capacity-factor, or backfill
   transformations;
4. merges prior ReEDS inputs and converts historic cost values to the configured
   dollar year;
5. writes each distinct scenario to `../output/`.

When enabled, it also creates system and technology financial files. When
`copy_to_reeds` is true, generated files are copied into the configured ReEDS
repository.

## Commands

```bash
# Follow config.yaml
python generate_atb_files.py

# Override the configured technology selection
python generate_atb_files.py -t battery upv

# Explicitly skip financial or cost outputs
python generate_atb_files.py --skip_financials
python generate_atb_files.py --skip_costs
```

`-f` means `--skip_financials`; it does not mean force.
