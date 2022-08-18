library(tidyverse)
library(readxl)
library(popmodules)

fpaths <- list(deaths = "Q:/Teams/D&PA/Data/births_and_deaths/Mid year OA deaths FINAL.xlsx",
               lookup = "lookups/OA11_LSOA11_MSOA11_LAD11.csv",
               output = "processed/deaths_by_year/oa_deaths_all_EW_2002-2020_totals.rds")

dir.create("processed/deaths_by_year", showWarnings = FALSE)

#-------------------------------------------------------------------------------

deaths_excel <- read_excel(fpaths$deaths,
                           sheet = 3, skip = 2)

lookup <- fread(fpaths$lookup) %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, LAD21CD = LAD11CD) %>% 
  recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE, col_geog = "LAD21CD")

deaths_oa <- deaths_excel %>% 
  pivot_longer(cols = -c('Output Area', Sex), names_to = "year", values_to = "deaths") %>%
  mutate(sex = recode(Sex, '1' = "male", '2' = "female"),
         year = as.numeric(substr(year,6,9))) %>% 
  rename(OA11CD = 'Output Area') %>% 
  complete(OA11CD = unique(lookup$OA11CD), year, sex, fill = list(deaths = 0)) %>% 
  left_join(lookup, by = "OA11CD") %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, deaths)

sum(is.na(deaths_oa))
length(unique(deaths_oa$OA11CD))

saveRDS(deaths_oa, fpaths$output)

#-------------------------------------------------------------------------------



