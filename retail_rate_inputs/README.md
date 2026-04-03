## Summary
The script in this folder (`trim_retail_rate_inputs.py`) trims the retail rate inputs that come from FERC Form 1. This is done to reduce the size of these files in the ReEDS repository. Only data that is used by the retail rate module in ReEDS is retained in the trimmed files.

## Use of script
Simply run `trim_retail_rate_inputs.py` and then copy the files from `Outputs-trimmed` to `postprocessing\retail_rate_module\inputs` in the ReEDS repository.

## Data source
The three datasets in the `Inputs` folder are all pulled from FERC Form 1. Specifics for that process is provided in [Retail Rate Projections for Long-Term Electricity System Models](https://docs.nlr.gov/docs/fy22osti/78224.pdf). See sections 2.2.2 and 2.2.3 of that report.
