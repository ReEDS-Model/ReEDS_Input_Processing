These updates were done 05/31/2024 by Wesley Cole and Max Vanatta and 02/28/2025 by Kodi Obika.

The GSw_Canada switch for cases in ReEDS allow for:
0: No Canadian trade
1: Seasonal balancing of Canadian electricity trade which uses the annual balances (can_exports.csv and can_imports.csv in outputs folder, which contains inputs to ReEDS-2.0) and seasonal fractions (can_exports_szn_frac.csv and can_imports_szn_frac.csv in outputs folder). Operated simnilar to Hydro.
2: (not implemented in Current ReEDS 2.0) Strict hourly limits defined by can_trade_8760.h5

To generate updated input files for option 1 using Canadian_import_export_formatter.py:
- Download the historical import and export data from the Canadian government: https://www.cer-rec.gc.ca/en/data-analysis/energy-commodities/electricity/statistics/electricity-trade-summary/index.html
- Put this file in the "data/raw_inputs" directory.
-- check to ensure the sheet name for monthly import/export matches the string in the python file. (line 17)

- Download the projections for future province level import and export from the Canadian government: https://apps.cer-rec.gc.ca/ftrppndc/dflt.aspx?GoCTemplateCulture=en-CA
- Put this file in the "data/raw_inputs" directory.

- Ensure that the mapping of imports and exports between provinces and counties match what would be desired in the province_to_county_map.csv file.
-- This file allocates what ratio of each province is associated with each county.

- Run Canadian_import_export_formatter.py in the "Exogenous_Canadian_Trade" directory.

Option 2 has been deprecated in the current version of ReEDS (2024.4.0), so the .h5 file used for that option is not updated at this time (June 2024).


To update the province-to-county map:
- Download the "Border Crossings - Electric Transmission Line" dataset from the EIA: https://www.eia.gov/trilateral/#!/maps. 
- Move and rename the file to "data/raw_inputs/EIA_trilateral_lines.csv".
- Run process_eia_lines.py to create "data/EIA_usa_can_lines.csv".

- Download the "Sheet 1" Excel file from the Canadian government's "International Power Lines" dashboard: https://www.cer-rec.gc.ca/en/data-analysis/facilities-we-regulate/international-power-lines-dashboard/.
- Move and rename the file to "data/raw_inputs/CER_usa_can_lines.xlsx".

- Using characteristics specified in each dataset, manually create a mapping between the `line_ID`s listed in EIA_usa_can_lines.csv and the `Original regulatory instrument`s listed in CER_usa_can_lines.xlsx.
- Save the mapping to "data/line_id_certificate_map.json".
- Note that there are some inconsistencies between the EIA and CER datasets:
-- In cases where EIA lists multiple lines where CER lists only one, we assume the CER line represents a combination of EIA lines (and therefore the "Capacity" column is the total capacity of the EIA lines).
-- In cases where EIA lists one line where CER lists multiple, we assume the EIA line represents a combination of CER lines (and therefore the sum of the CER lines' "Capacity" columns is the capacity of the EIA line).
-- In cases where the U.S. state listed in the datasets do not match, we use EIA's provided location, as it has county-level information.

- Run create_province_to_county_map.py in the "Exogenous_Canadian_Trade" directory.
