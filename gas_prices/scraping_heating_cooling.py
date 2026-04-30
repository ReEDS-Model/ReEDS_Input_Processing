####################################READ FIRST##############################################
#Only run this script if you would like to update the numbers in heating_cooling.csv #######
#The current file was aggregated in April of 2026                                    #######
############################################################################################

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

#################################################################################
############### Cooling data ####################################################

#read in the cooling data
cooling_data = pd.read_csv('data/cooling_degree_days_noaa.csv')
#replace anything that isn't a number with a comma
cooling_data = cooling_data.replace(r'[^\d]+', ',', regex=True)
#split the data into columns
cooling_data = cooling_data['1'].str.split(',', expand=True)
#create header row aggregated, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
cooling_data.columns = ['aggregated', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'empty']
#divide the first columns into 4 columns, [0:3], [3], [4:6], [6:]
cooling_data['state_code'] = cooling_data['aggregated'].str[0:3]
cooling_data['division_number'] = cooling_data['aggregated'].str[3]
cooling_data['element_code'] = cooling_data['aggregated'].str[4:6]
cooling_data['year'] = cooling_data['aggregated'].str[6:]
#drop the aggregated column
cooling_data = cooling_data.drop(columns=['aggregated', 'division_number', 'empty'])


#keep only 25 and 26 in element code
cooling_data = cooling_data[cooling_data['element_code'].isin(['25', '26'])]
fips_to_state = {'001': 'Alabama',         '030': 'New York',
          '002': 'Arizona',         '031': 'North Carolina',
          '003': 'Arkansas',        '032': 'North Dakota',
          '004': 'California',      '033': 'Ohio',
          '005': 'Colorado',        '034': 'Oklahoma',
          '006': 'Connecticut',     '035': 'Oregon',
          '007': 'Delaware',        '036': 'Pennsylvania',
          '008': 'Florida',         '037': 'Rhode Island',
          '009': 'Georgia',         '038': 'South Carolina',
          '010': 'Idaho',           '039': 'South Dakota',
          '011': 'Illinois',        '040': 'Tennessee',
          '012': 'Indiana',         '041': 'Texas',
          '013': 'Iowa',            '042': 'Utah',
          '014': 'Kansas',          '043': 'Vermont',
          '015': 'Kentucky',        '044': 'Virginia',
          '016': 'Louisiana',       '045': 'Washington',
          '017': 'Maine',           '046': 'West Virginia',
          '018': 'Maryland',        '047': 'Wisconsin',
          '019': 'Massachusetts',   '048': 'Wyoming',
          '020': 'Michigan',        '049': 'Hawaii',
          '021': 'Minnesota',       '050': 'Alaska',
          '022': 'Mississippi',     
          '023': 'Missouri',        
          '024': 'Montana',         
          '025': 'Nebraska',        
          '026': 'Nevada',          
          '027': 'New Hampshire',   
          '028': 'New Jersey',      
          '029': 'New Mexico',      }
#drop all rows not state level
cooling_data = cooling_data[cooling_data['state_code'].isin(fips_to_state.keys())]
#add a column with the state name
cooling_data['state'] = cooling_data['state_code'].map(fips_to_state)
cooling = cooling_data[(cooling_data['state'] != 'Hawaii')&(cooling_data['state'] != 'Alaska')]
cooling['year'] = cooling['year'].astype(int)
cooling = cooling[(cooling['year'] >=1997)&(cooling['year'] <=2025)]
#################################################################################
############### Heating Data ######################################################

#read in the heating data
heating_data = pd.read_csv('data/heating_degree_days_noaa.csv')
#replace anything that isn't a number with a comma
heating_data = heating_data.replace(r'[^\d]+', ',', regex=True)
#split the data into columns
heating_data = heating_data['1'].str.split(',', expand=True)
#create header row aggregated, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
heating_data.columns = ['aggregated', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'empty']
#divide the first columns into 4 columns, [0:3], [4], [5:7], [7:]
heating_data['state_code'] = heating_data['aggregated'].str[0:3]
heating_data['division_number'] = heating_data['aggregated'].str[3]
heating_data['element_code'] = heating_data['aggregated'].str[4:6]
heating_data['year'] = heating_data['aggregated'].str[6:]
#drop the aggregated column
heating_data = heating_data.drop(columns=['aggregated', 'division_number', 'empty'])

#keep only 25 and 26 in element code
heating_data = heating_data[heating_data['element_code'].isin(['25', '26'])]
fips_to_state = {'001': 'Alabama',         '030': 'New York',
          '002': 'Arizona',         '031': 'North Carolina',
          '003': 'Arkansas',        '032': 'North Dakota',
          '004': 'California',      '033': 'Ohio',
          '005': 'Colorado',        '034': 'Oklahoma',
          '006': 'Connecticut',     '035': 'Oregon',
          '007': 'Delaware',        '036': 'Pennsylvania',
          '008': 'Florida',         '037': 'Rhode Island',
          '009': 'Georgia',         '038': 'South Carolina',
          '010': 'Idaho',           '039': 'South Dakota',
          '011': 'Illinois',        '040': 'Tennessee',
          '012': 'Indiana',         '041': 'Texas',
          '013': 'Iowa',            '042': 'Utah',
          '014': 'Kansas',          '043': 'Vermont',
          '015': 'Kentucky',        '044': 'Virginia',
          '016': 'Louisiana',       '045': 'Washington',
          '017': 'Maine',           '046': 'West Virginia',
          '018': 'Maryland',        '047': 'Wisconsin',
          '019': 'Massachusetts',   '048': 'Wyoming',
          '020': 'Michigan',        '049': 'Hawaii',
          '021': 'Minnesota',       '050': 'Alaska',
          '022': 'Mississippi',     
          '023': 'Missouri',        
          '024': 'Montana',         
          '025': 'Nebraska',        
          '026': 'Nevada',          
          '027': 'New Hampshire',   
          '028': 'New Jersey',      
          '029': 'New Mexico',      }
#drop all rows not state level
heating_data = heating_data[heating_data['state_code'].isin(fips_to_state.keys())]
#add a column with the state name
heating_data['state'] = heating_data['state_code'].map(fips_to_state)
heating = heating_data[(heating_data['state'] != 'Hawaii')&(heating_data['state'] != 'Alaska')]
heating['year'] = heating['year'].astype(int)
heating = heating[(heating['year'] >=1997)&(heating['year'] <=2025)]


#################################################################################
############### Include DC ######################################################

BASE_URL = (
    "https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/"
    "cdus/degree_days/archives/Cooling%20Degree%20Days/"
    "monthly%20cooling%20degree%20days%20state/"
)
months_order = ["jan", "feb", "mar", "apr", "may", "jun",
                "jul", "aug", "sep", "oct", "nov", "dec"]

month_map = {
    "jan": "Jan",
    "feb": "Feb",
    "mar": "Mar",
    "apr": "Apr",
    "may": "May",
    "jun": "Jun",
    "jul": "Jul",
    "aug": "Aug",
    "sep": "Sep",
    "oct": "Oct",
    "nov": "Nov",
    "dec": "Dec",
}

months = list(month_map.keys())

def get_month_file_map(year):
    """
    Scrape the year folder and map month -> actual file URL.
    Handles weird names like:
      aug 97.txt
      Oct 1997.txt
      dec 1997.txt
    """
    year_url = f"{BASE_URL}{year}/"
    r = requests.get(year_url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    out = {}

    for a in soup.find_all("a"):
        href = a.get("href", "").strip()
        # if not href.lower().endswith(".txt"):
        #     continue
        if href.lower().endswith("rectory"):
            continue

        href_lower = href.lower()

        for mon in months_order:
            # match month abbreviation anywhere in filename
            if re.search(rf"\b{mon}\b", href_lower):
                out[mon] = urljoin(year_url, href)
                break

    return out


def extract_dc_month_total(file_url):
    r = requests.get(file_url, timeout=30)
    r.raise_for_status()

    text = re.sub(r"\s+", " ", r.text)

    m = re.search(r"\bDISTRCT COLUMBIA\s+(-?\d+)\b", text)
    if not m:
        return pd.NA

    return int(m.group(1))


rows = []
for year in range(1997, 2026):
    row = {mon: pd.NA for mon in months_order}
    file_map = get_month_file_map(year)

    for mon in months_order:
        if mon in file_map:
            row[mon] = extract_dc_month_total(file_map[mon])

    row["state_code"] = "11"
    row["element_code"] = "26"
    row["year"] = year
    row["state"] = "District of Columbia"
    rows.append(row)

c = pd.DataFrame(
    rows,
    columns=months_order + ["state_code", "element_code", "year", "state"]
)

for mon in months_order:
    c[mon] = pd.to_numeric(c[mon], errors="coerce").astype("Int64")



BASE_URL = (
    "https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/"
    "cdus/degree_days/archives/Heating%20degree%20Days/monthly%20states/"
)

months_order = ["jan", "feb", "mar", "apr", "may", "jun",
                "jul", "aug", "sep", "oct", "nov", "dec"]

month_lookup = {
    "jan": "jan",
    "feb": "feb",
    "mar": "mar",
    "apr": "apr",
    "may": "may",
    "jun": "jun",
    "jul": "jul",
    "aug": "aug",
    "sep": "sep",
    "oct": "oct",
    "nov": "nov",
    "dec": "dec",
}

def get_month_file_map(year):
    """
    Scrape the year folder and map month -> actual file URL.
    Handles weird names like:
      aug 97.txt
      Oct 1997.txt
      dec 1997.txt
    """
    year_url = f"{BASE_URL}{year}/"
    r = requests.get(year_url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    out = {}

    for a in soup.find_all("a"):
        href = a.get("href", "").strip()
        if not href.lower().endswith(".txt"):
            continue

        href_lower = href.lower()

        for mon in months_order:
            # match month abbreviation anywhere in filename
            if re.search(rf"\b{mon}\b", href_lower):
                out[mon] = urljoin(year_url, href)
                break

    return out


def extract_dc_month_total(file_url):
    r = requests.get(file_url, timeout=30)
    r.raise_for_status()

    text = re.sub(r"\s+", " ", r.text)

    m = re.search(r"\bDISTRCT COLUMBIA\s+(-?\d+)\b", text)
    if not m:
        return pd.NA

    return int(m.group(1))


rows = []
for year in range(1997, 2026):
    row = {mon: pd.NA for mon in months_order}
    file_map = get_month_file_map(year)

    for mon in months_order:
        if mon in file_map:
            row[mon] = extract_dc_month_total(file_map[mon])

    row["state_code"] = "11"
    row["element_code"] = "25"
    row["year"] = year
    row["state"] = "District of Columbia"
    rows.append(row)

h = pd.DataFrame(
    rows,
    columns=months_order + ["state_code", "element_code", "year", "state"]
)

for mon in months_order:
    h[mon] = pd.to_numeric(h[mon], errors="coerce").astype("Int64")
#################################################################################
############### Combine all######################################################

heating.drop(columns=['state_code'], inplace=True)
cooling.drop(columns=['state_code'], inplace=True)
c.drop(columns=['state_code'], inplace=True)
h.drop(columns=['state_code'], inplace=True)

#concatenate the heating and cooling dataframes
combined = pd.concat([heating, cooling, h, c], ignore_index=True)
#make map of state to state abbrev
state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC', 
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

combined['state_abbrev'] = combined['state'].map(state_abbrev)
combined.to_csv("heating_cooling.csv", index=False)
