# Plotting stage

`atb_plotting.py` reads the same local raw flat file used by the ReEDS formatter.
It does not prompt for URLs, download data, depend on external JSON style files,
or require importing plotting utilities from a separate ReEDS checkout.

Configure metrics, case, CRP years, technologies, output directory, and image
format under `plotting:` in `../config.yaml`, then run:

```bash
python atb_plotting.py
```

Override the configured metric list when needed:

```bash
python atb_plotting.py --metric CAPEX "Fixed O&M"
```

Moderate values are drawn as lines. Where both Advanced and Conservative data
exist, the range between them is shown as a band. Figures are saved to the
configured plotting output directory.
