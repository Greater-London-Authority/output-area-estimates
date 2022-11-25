library(dplyr)
library(data.table)

# File Paths and read in data

tm <- Sys.time()

round_value <- 3 # decimal places for outputs

Q_data_folder <- "Q:/Teams/D&PA/Data/population_estimates/"
Q_census_folder <- "Q:/Teams/D&PA/Census/2021 Census/central_data_folder/"
dir.create("processed/2021_series", showWarnings = FALSE)

f_paths <- list(lookup_2011 = "lookups/OA11_LSOA11_MSOA11_LAD21.rds",
                lookup_2021 =  paste0(Q_census_folder, "geog_lookups/2021_oa_lsoa_msoa_la.csv"),
                oa11_to_msoa21 =  paste0(Q_census_folder, "geog_lookups/2011_oa_2021_msoa_E&W.csv"),
                new_la_series = "Q:/Teams/D&PA/Demography/MYE/gla_revised_mye_series.rds",
                oa_in = "processed/population_by_year/oa_population_all_EW_GLA_series_",
                lsoa_in = paste0(Q_data_folder, "ons_small_area_population_estimates/population_LSOA11_01_to_20.rds"),
                msoa_in = paste0(Q_data_folder, "ons_small_area_population_estimates/population_MSOA11_01_to_20.rds"),
                census_msoa =  paste0(Q_census_folder, "raw_data/2021/1. Demography and migration/msoa/age_101a.csv"),
                oa_out = "processed/2021_series/population_by_year/",
                lsoa_out = "processed/2021_series/lsoa_population_all_EW_2021_series.rds",
                msoa_out = "processed/2021_series/msoa_population_all_EW_2021_series.rds")

#---

#Prep existing series at different levels for re-scaling

lookup_2011 <- readRDS(f_paths$lookup_2011)
lookup_2021 <- fread(f_paths$lookup_2021) %>% data.frame() %>% 
  select(OA21CD = oa21cd, LSOA21CD = lsoa21cd, MSOA21CD = msoa21cd, LAD21CD = lad22cd)

la_in <- readRDS(f_paths$new_la_series)

new_LA_series <- la_in %>% 
  rename(new_la = value,
         LAD21CD = gss_code) %>% 
  filter(year %in% 2012:2020, component == "population") %>%
  filter(substr(LAD21CD,2,3) %in% c("06","07","08","09")) %>% 
  select(LAD21CD, year, sex, age, new_la) %>% 
  mutate(new_la = ifelse(new_la < 0, 0, new_la)) # TODO this is temp

#---

# Scale existing series to match new series at 1 geography above
# i.e. Scale MSOA to LAD, LSOA to MSOA, OA to LSOA
# Cascade through

#MSOA

msoa_in <- readRDS(f_paths$msoa_in) %>% data.frame()

existing_msoa_series <- msoa_in %>% 
  dtplyr::lazy_dt() %>%
  filter(substr(MSOA11CD,1,1)!="W") %>%  # TODO Add wales back in
  filter(year %in% 2012:2020) %>% 
  select(MSOA11CD, year, sex, age, popn = population) %>% 
  left_join(distinct(select(lookup_2011, MSOA11CD, LAD21CD)), by = "MSOA11CD") %>% 
  group_by(LAD21CD, year, sex, age) %>% 
  mutate(la_total = sum(popn)) %>% 
  data.frame()

new_msoa_series <- existing_msoa_series %>% 
  left_join(new_LA_series, by = c("LAD21CD","year","sex","age")) %>%
  mutate(scaling = ifelse(la_total == 0, 0, new_la/la_total),
         rescaled_msoa = popn*scaling)

new_msoa_total <- new_msoa_series %>% 
  group_by(LAD21CD, year) %>% 
  summarise(msoa_total = sum(rescaled_msoa),
            .groups = 'drop_last') %>% 
  data.frame() 

la_scaling <-  new_LA_series %>% 
  group_by(LAD21CD, year) %>% 
  summarise(LAD_total = sum(new_la),
            .groups = 'drop_last') %>% 
  data.frame() %>% 
  left_join(new_msoa_total, by = c("LAD21CD","year")) %>% 
  mutate(scaling2 = LAD_total/msoa_total)

new_msoa_series <- new_msoa_series %>% 
  left_join(la_scaling, by = c("LAD21CD","year")) %>% 
  mutate(rescaled_msoa2 = rescaled_msoa*scaling2) %>% 
  select(MSOA11CD, year, sex, age, rescaled_msoa2) %>% 
  rename(popn = rescaled_msoa2)

sum(new_msoa_series$popn)
sum(new_LA_series$new_la)
sum(is.na(new_msoa_series))

saveRDS(new_msoa_series, f_paths$msoa_out)

rm(existing_msoa_series, la_in, msoa_in, new_LA_series,
   new_msoa_total, la_scaling)
gc()

#---

#LSOA

lsoa_in <- readRDS(f_paths$lsoa_in) %>% data.frame()

existing_lsoa_series <- lsoa_in %>% 
  dtplyr::lazy_dt() %>%
  filter(year %in% 2012:2020) %>% 
  select(LSOA11CD, MSOA11CD, year, sex, age, popn = population) %>% 
  group_by(MSOA11CD, year, sex, age) %>% 
  mutate(msoa_total = sum(popn)) %>% 
  data.frame()

new_msoa_series <-  rename(new_msoa_series, rescaled_msoa = popn)

new_lsoa_series <- left_join(existing_lsoa_series, new_msoa_series, by = c("MSOA11CD","year","sex","age")) %>%
  filter(substr(LSOA11CD,1,1) != "W") %>% # TODO Figure out Wales
  mutate(scaling = ifelse(msoa_total == 0, 0, rescaled_msoa/msoa_total),
         rescaled_lsoa = popn*scaling) %>% 
  select(LSOA11CD, year, sex, age, rescaled_lsoa) %>% 
  rename(popn = rescaled_lsoa)

sum(new_lsoa_series$popn)
sum(new_msoa_series$rescaled_msoa)
sum(is.na(new_lsoa_series))

saveRDS(new_lsoa_series, f_paths$lsoa_out)

rm(lsoa_in, existing_lsoa_series)
gc()
#---

#OA

# in a loop because its so big

new_lsoa_series <-  rename(new_lsoa_series, rescaled_lsoa = popn)
dir.create(f_paths$oa_out, showWarnings = FALSE)

#yr <- 2020
for(yr in 2012:2020){
  
  message(yr)
  
  oa_in <- readRDS(paste0(f_paths$oa_in, yr, ".rds"))
  
  existing_oa_series <- oa_in %>%
    dtplyr::lazy_dt() %>%
    filter(substr(OA11CD,1,1) != "W") %>% # TODO Sort Wales out
    group_by(LSOA11CD, year, sex, age) %>% 
    mutate(lsoa_total = sum(popn)) %>% 
    data.frame()
  
  new_oa_series <- left_join(existing_oa_series, new_lsoa_series, by = c("LSOA11CD","year","sex","age")) %>%
    mutate(scaling = ifelse(lsoa_total == 0, 0, rescaled_lsoa/lsoa_total),
           rescaled_oa = popn*scaling) %>% 
    select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, rescaled_oa) %>% 
    rename(popn = rescaled_oa) %>%
    mutate(popn = round(popn, round_value))
  
  saveRDS(new_oa_series, paste0(f_paths$oa_out, "oa_population_all_EW_", yr, ".rds"))
  
  if(yr != 2020){ rm(new_oa_series) }
  rm(oa_in, existing_oa_series)
  gc()
  
}

#---

#2010 & 2011 is the same as the existing GLA series
# file.copy("processed/population_by_year/oa_population_all_EW_2010.rds", 
#           "processed/2021_series/population_by_year/oa_population_all_EW_2010.rds")
# 
# file.copy("processed/population_by_year/oa_population_all_EW_GLA_series_2011.rds", 
#           "processed/2021_series/population_by_year/oa_population_all_EW_2011.rds")

#TODO This is temporary until we have Wales

readRDS("processed/population_by_year/oa_population_all_EW_2010.rds") %>% 
  filter(substr(OA11CD,1,1)=="E") %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, popn) %>% 
  saveRDS("processed/2021_series/population_by_year/oa_population_all_EW_2010.rds")

readRDS("processed/population_by_year/oa_population_all_EW_GLA_series_2011.rds") %>% 
  filter(substr(OA11CD,1,1)=="E") %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, popn) %>% 
  saveRDS("processed/2021_series/population_by_year/oa_population_all_EW_2011.rds")

#-------------------------------------------------------------------------------


#2021 is different because we can't start with an existing OA estimate
#We do have the census estimate for MSOAs though

oa11_to_msoa21 <- fread(f_paths$oa11_to_msoa21) %>% data.frame() %>% 
  E00175033

new_LA_series <- readRDS(f_paths$new_la_series) %>% 
  filter(year == 2021) %>%
  rename(popn = value,
         LAD21CD = gss_code) %>% 
  filter(component == "population") %>%
  filter(substr(LAD21CD,2,3) %in% c("06","07","08","09")) %>% 
  select(LAD21CD, year, sex, age, popn)

new_LA_series_age <- new_LA_series %>% 
  group_by(LAD21CD, age) %>% 
  summarise(popn = sum(popn), .groups = 'drop_last') %>% 
  data.frame()


#Scale the census MSOA data to match the 2021 LAD estimate
census_msoa <- fread(f_paths$census_msoa) %>% 
  filter(substr(MSOA21CD,1,1)=="E") %>%  # TODO Sort out Wales
  data.frame() %>% 
  mutate(age = ifelse(age > 90, 90, age)) %>% 
  group_by(MSOA21CD, age) %>% 
  summarise(count = sum(count), .groups = 'drop_last') %>% 
  data.frame() %>% 
  left_join(distinct(select(lookup_2021, MSOA21CD, LAD21CD)), by = "MSOA21CD") %>% 
  group_by(LAD21CD, age) %>% 
  mutate(msoa_proportion = count/sum(count)) %>% 
  data.frame() %>% 
  left_join(new_LA_series_age, by = c("LAD21CD","age")) %>% 
  mutate(scaled_popn = popn * msoa_proportion) %>% 
  select(MSOA21CD, age, scaled_popn) %>% 
  rename(popn = scaled_popn)

sum(census_msoa$popn)
sum(new_LA_series$popn)
sum(is.na(census_msoa))

#Scale the 2020 OA popn to the 2021 MSOA pop
oa_2021 <- left_join(new_oa_series, oa11_to_msoa21, by = "OA11CD") %>%
  mutate(year = 2021) %>% 
  group_by(MSOA21CD, age) %>% 
  mutate(msoa_total = sum(popn)) %>% 
  data.frame() %>% 
  left_join(census_msoa, by = c("MSOA21CD","age")) %>%
  rename(popn = popn.x,
         census_popn = popn.y) %>% 
  mutate(scaling = ifelse(msoa_total == 0, 0, census_popn/msoa_total),
         rescaled_oa = popn*scaling) %>% 
  select(OA11CD, LSOA11CD, MSOA21CD, LAD21CD, year, sex, age, rescaled_oa) %>% 
  rename(popn = rescaled_oa)

# 2x 2021 MSOAs have no 2011 OA centroid within them
# This MSOA population is therefore missing from the above data frame
missing_MSOAs <- setdiff(census_msoa$MSOA21CD, oa_2021$MSOA21CD)

#Identified the OA11 that the MSOA21 sits within on GIS
missing_lookup <- data.frame(OA11CD = c("E00175033", "E00100663"),
                             MSOA21CD = missing_MSOAs)

missing_popn <- filter(census_msoa, MSOA21CD %in% mssing_MSOAs) %>% 
  left_join(missing_lookup, by = "MSOA21CD") %>% 
  rename(missing = popn) %>% 
  select(-MSOA21CD) %>% 
  right_join(oa_2021, by = c("OA11CD", "age")) 

oa_2021 <- missing_popn %>% 
  filter(OA11CD %in% missing_lookup$OA11CD) %>% 
  group_by(OA11CD, age) %>% 
  mutate(sex_ratio = popn/sum(popn),
         sex_ratio = ifelse(is.nan(sex_ratio), 0.5, sex_ratio)) %>% 
  data.frame() %>% 
  mutate(popn = popn + (missing*sex_ratio)) %>% 
  select(names(oa_2021)) %>% 
  bind_rows(filter(missing_popn, !OA11CD %in% missing_lookup$OA11CD))

#Scale to LA at the total population level
lad_total <- new_LA_series %>% 
  group_by(LAD21CD) %>% 
  summarise(lad_total = sum(popn), .groups = 'drop_last') %>% 
  data.frame()

oa_2021_final <- oa_2021 %>%
  group_by(LAD21CD) %>% 
  mutate(oa_summed = sum(popn)) %>% 
  data.frame() %>% 
  left_join(lad_total, by = "LAD21CD") %>%
  mutate(scaling = ifelse(oa_summed == 0, 0, lad_total/oa_summed),
         rescaled_oa = popn*scaling) %>% 
  select(OA11CD, LSOA11CD, MSOA21CD, LAD21CD, year, sex, age, rescaled_oa) %>% 
  rename(popn = rescaled_oa)

# Check that the sum of all OAs is about equal to the sum of all LAs
sum(oa_2021_final$popn)
sum(census_msoa$popn)
sum(new_LA_series$popn)
sum(is.na(oa_2021_final))

saveRDS(oa_2021_final, paste0(f_paths$oa_out, "oa_population_all_EW_2021.rds"))

#---

tm2 <- Sys.time()
print(tm2-tm)
