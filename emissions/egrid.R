library(data.table)
library(readxl)
library(sf)
library(reedling)
library(ggplot2)
library(dplyr)

## paths

# path to this script
setwd("~/Documents/github/ReEDS-2.0_Input_Processing/Emissions")

# set this path to your ReEDS-2.0 repo 
reedspath <- "/Users/bsergi/Documents/github/ReEDS-2.0"


## load data ###
# read in egrid data
egrid <- data.table(read_excel("egrid2022_data.xlsx", sheet="PLNT22", skip = 1))

# subset to plant name, FIPS (state + county), and CO2 emissions
egrid_sub <- egrid[, c("PNAME", "ORISPL", "SECTOR", "PLFUELCT", "NAMEPCAP", "PSTATABB", "FIPSST", "FIPSCNTY", "LAT", "LON", "PLCO2AN")]

# drop AK, HI, and PR
egrid_sub <- egrid_sub[!(PSTATABB %in% c("AK", "HI", "PR"))]

# subset to electric utilities
egrid_sub <- egrid_sub[SECTOR %in% c("Electric Utility", "IPP Non-CHP", "IPP CHP")]

# check on total CO2 (convert from short tons)
short_to_metric_ton <- 1.10231
total_CO2 <-sum(egrid_sub$PLCO2AN, na.rm = T)

total_CO2 / 1e6                         # short tons
total_CO2  / 1e6 / short_to_metric_ton  # metric tons

# total is ~1,500 million metric tonnes
# in line with the estimate here: https://www.epa.gov/ghgemissions/sources-greenhouse-gas-emissions#electricity

## compute fractions ####

## Option 1: use FIPS code in eGrid data
# get egrid FIPS code
# egrid_sub$fips_egrid <- with(egrid_sub, paste0("p", FIPSST, FIPSCNTY))
# 
# # compute percentages by county
# ba_co2_share <- egrid_sub[, by=.(fips_egrid), .(share=(sum(PLCO2AN,na.rm = T)/total_CO2))]

# Note: this does not include recent county updates (e.g., CT), so go with option 2

## Option 2: map lat/longs to counties using shapefile
cnty_shp <- load_ba_shapefile(reedspath, "US_COUNTY_2022")  # File no longer in reeds repo

egrid_match <- st_as_sf(x = egrid_sub,                         
                        coords = c("LON", "LAT"),
                        crs = "EPSG:4326")
egrid_match <- st_transform(egrid_match, st_crs(cnty_shp))

st_shp <- sf::st_buffer(cnty_shp,0) %>%
  group_by(STATE, STCODE) %>%
  summarize(geometry = st_union(geometry))

# quick sanity check
ggplot()+
  geom_sf(data=st_shp, mapping=aes(), fill=NA, linewidth=0.6, color="black") +
  geom_sf(data=egrid_match, mapping=aes(color=PSTATABB)) +
  theme_bw()

# now match points to counties
egrid_matched <- st_intersection(egrid_match, cnty_shp)

# extract relevant results to data.table
egrid_matched_out <- data.table(egrid_matched[, c("PNAME", "PSTATABB", "PLCO2AN", "r")])

## look at cases where the lat/lon matches to a different county than in eGrid
# mismatches <- egrid_matched_out[fips_egrid!=r & !is.na(PLCO2AN)]

# add interconnection
hierarchy <- fread(file.path(reedspath, "inputs", "hierarchy.csv"))
cnty2one <- fread(file.path(reedspath, "inputs", "county2zone.csv"))
cnty_int <- merge(cnty2one, hierarchy[country=="USA"], by="ba", all=T)
cnty_int$r <- paste0("p", sprintf("%05d", cnty_int$FIPS))
egrid_matched_out <- merge(egrid_matched_out, cnty_int[, c("r", "interconnect")])

# now calculate share of emissions by county
ba_co2_share_matched <- egrid_matched_out[, by=.(r), .(share=(sum(PLCO2AN,na.rm = T)/total_CO2))]

# drop zeros--these occur by matching eGrid plants with no emissions to counties
ba_co2_share_matched <- ba_co2_share_matched[share > 0]

## data checks ####

# check shares sum to 1
sum(ba_co2_share_matched$share)

# check interconnection sums
int_co2_share_matched <- egrid_matched_out[, by=.(interconnect), .(share=(sum(PLCO2AN,na.rm = T)/total_CO2))]
int_co2_share_matched

# write out data
write.csv(ba_co2_share_matched, "county_co2_share_egrid_2022.csv", row.names = F)



