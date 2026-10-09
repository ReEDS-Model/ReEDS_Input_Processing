"""Extract ReEDS inputs from a downloaded ATB workbook.

The battery sheet supplies the power/energy cost split the flat file never
publishes. Technology sheets (Biopower, Nuclear, Coal_FE, ...) supply any
series the flat file omits, such as Biopower - 95% CCS in ATB 2025.
"""

from pathlib import Path

import openpyxl
import pandas as pd


BATTERY_SHEET = "Utility-Scale Battery Storage"
BLOCKS = {
    "Capital Cost ($/kWh)": "capcost_energy",
    "Battery Power Capital Cost ($/kW)": "capcost",
}
SCENARIOS = ["Advanced", "Moderate", "Conservative"]

# Technology sheets list each metric as a block: a year header row, then one
# row per display name, financial case, and scenario. These are the metric
# labels mapped onto the flat file's core_metric_parameter names.
SHEET_PARAMETERS = {
    "Net Capacity Factor (%)": "CF",
    "Heat Rate (MMBtu/MWh)": "Heat Rate",
    "CAPEX ($/kW)": "CAPEX",
    "Construction Financing Cost ($/kW)": "CFC",
    "Overnight Capital Cost ($/kW)": "OCC",
    "Fixed Operation and Maintenance Expenses ($/kW-yr)": "Fixed O&M",
    "Variable Operation and Maintenance Expenses ($/MWh)": "Variable O&M",
    "Grid Connection Costs (GCC) ($/kW)": "GCC",
}
# Financial-case labels on the technology sheets and the flat file Case each
# one corresponds to.
SHEET_CASES = {
    "R&D Cost Drivers Without Tax Credits": "R&D",
    "Expanded Cost Drivers Without Tax Credits": "Exp",
}


def extract_sheet_parameters(xlsx_path, sheet_name, technology):
    """Return one technology sheet's metrics in the flat file's long format.

    The frame carries the columns the formatter subsets on (Technology,
    DisplayName, Case, Scenario, Parameter, variable, value), so a technology
    read from the workbook is formatted exactly like one read from the flat
    file. The sheets state no capital recovery period for these metrics, so
    there is no CRPYears column. Whatever the sheet does not state is an
    error; nothing is filled in from elsewhere.
    """
    workbook_path = Path(xlsx_path)
    workbook = openpyxl.load_workbook(
        workbook_path, read_only=True, data_only=True
    )
    try:
        if sheet_name not in workbook.sheetnames:
            raise ValueError(
                f"Sheet {sheet_name!r} not found in {workbook_path}; "
                f"sheets: {workbook.sheetnames}"
            )
        rows = list(workbook[sheet_name].iter_rows(values_only=True))
    finally:
        workbook.close()

    def is_year(value):
        return (
            isinstance(value, (int, float)) and not isinstance(value, bool)
            and float(value).is_integer() and 2000 <= value <= 2100
        )

    year_columns = None
    label = None
    records = []
    for row in rows:
        cells = list(row)
        years = [(i, int(v)) for i, v in enumerate(cells) if is_year(v)]
        if len(years) >= 10 and all(
            b - a == 1 for (_, a), (_, b) in zip(years, years[1:])
        ):
            year_columns = years
            label = None
            continue
        if year_columns is None:
            continue
        first = year_columns[0][0]
        if first < 4 or len(cells) <= first:
            continue
        metric, display, case, scenario = cells[first - 4:first]
        if isinstance(metric, str) and metric.strip():
            label = metric.strip()
        if (
            scenario not in SCENARIOS
            or not isinstance(display, str)
            or not isinstance(case, str)
        ):
            continue
        parameter = SHEET_PARAMETERS.get(label)
        if parameter is None:
            continue
        if case not in SHEET_CASES:
            raise ValueError(
                f"Unmapped financial case {case!r} on sheet {sheet_name!r}; "
                "add it to SHEET_CASES in atb_workbook.py."
            )
        for column, year in year_columns:
            value = cells[column] if column < len(cells) else None
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValueError(
                    f"Sheet {sheet_name!r}: {label!r} for {display!r} "
                    f"{scenario} has no value in {year}."
                )
            records.append({
                "Technology": technology,
                "DisplayName": display.strip(),
                "Case": SHEET_CASES[case],
                "Scenario": scenario,
                "Parameter": parameter,
                "variable": year,
                "value": float(value),
            })

    if not records:
        raise ValueError(
            f"Sheet {sheet_name!r} in {workbook_path} has no mapped metric "
            f"blocks; expected labels such as {list(SHEET_PARAMETERS)[:3]}."
        )
    return pd.DataFrame(records)


def extract_battery_costs(xlsx_path):
    """Return battery power/energy capital costs by scenario and year."""
    workbook_path = Path(xlsx_path)
    workbook = openpyxl.load_workbook(
        workbook_path, read_only=True, data_only=True
    )
    try:
        sheet_name = next((name for name in (
            BATTERY_SHEET, 'Utility-Scale Battery - Expand'
        ) if name in workbook.sheetnames), None)
        if sheet_name is None:
            raise ValueError(
                f"Sheet {BATTERY_SHEET!r} not found in {workbook_path}."
            )
        rows = list(workbook[sheet_name].iter_rows(values_only=True))
    finally:
        workbook.close()

    def find_label_row(label):
        for index, row in enumerate(rows):
            if any(
                isinstance(value, str) and value.strip() == label
                for value in row[:6]
            ):
                return index
        raise ValueError(f"Label {label!r} not found in {BATTERY_SHEET}.")

    records = []
    for label, cost_name in BLOCKS.items():
        label_row = find_label_row(label)
        year_header = rows[label_row + 1]
        year_columns = [
            (column, int(value))
            for column, value in enumerate(year_header)
            if isinstance(value, (int, float)) and 2018 <= value <= 2061
        ]
        if not year_columns:
            raise ValueError(f"No year header found below {label!r}.")

        for offset, scenario in enumerate(SCENARIOS):
            data_row = rows[label_row + 2 + offset]
            labels = [
                str(value).strip()
                for value in data_row[:5]
                if isinstance(value, str)
            ]
            if scenario not in labels:
                raise ValueError(
                    f"Expected scenario {scenario!r} below {label!r}, "
                    f"got {labels!r}."
                )
            record = {"cost": cost_name, "Scenario": scenario}
            record.update(
                {year: data_row[column] for column, year in year_columns}
            )
            records.append(record)

    frame = pd.DataFrame(records)
    year_columns = sorted(
        column for column in frame.columns if isinstance(column, int)
    )
    cost_order = {"capcost": 0, "capcost_energy": 1}
    scenario_order = {scenario: index for index, scenario in enumerate(SCENARIOS)}
    frame = (
        frame.assign(
            _cost_order=frame["cost"].map(cost_order),
            _scenario_order=frame["Scenario"].map(scenario_order),
        )
        .sort_values(["_cost_order", "_scenario_order"])
        .drop(columns=["_cost_order", "_scenario_order"])
        .reset_index(drop=True)
    )
    return frame[["cost", "Scenario", *year_columns]]
