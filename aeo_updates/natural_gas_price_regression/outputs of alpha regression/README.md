# Outputs of Alpha Regression

This directory contains the **final outputs** of the natural gas price regression pipeline.

## Copying outputs to ReEDS

Copy **all CSV files except `national_beta.csv`** to:

```
ReEDS/inputs/fuelprices/
```

For `national_beta.csv`, copy its value into `ReEDS/inputs/scalars.csv` under the key **`nat_beta_nonenergy`**.
