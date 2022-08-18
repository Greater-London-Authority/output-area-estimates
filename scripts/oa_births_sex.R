library(tidyverse)
library(readxl)
library(popmodules)


fpaths <- list(births = "Q:/Teams/D&PA/Data/births_and_deaths/Mid year OA births FINAL.xlsx",
               lookup = "lookups/OA11_LSOA11_MSOA11_LAD11.csv")

dir.create("processed/births_by_year", showWarnings = FALSE)

#-------------------------------------------------------------------------------
births_excel <- read_excel(fpaths$births,
                           sheet = 3, skip = 2)

lookup <- fread(fpaths$lookup) %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, LAD21CD = LAD11CD) %>% 
  recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE, col_geog = "LAD21CD")

births_oa <- births_excel %>% 
  pivot_longer(cols = -c('Output Area', Sex), names_to = "year", values_to = "births") %>%
  mutate(sex = recode(Sex, '1' = "male", '2' = "female"),
         year = as.numeric(substr(year,6,9)),
         age = 0) %>% 
  rename(OA11CD = 'Output Area') %>% 
  complete(OA11CD = unique(lookup$OA11CD), year, sex, age, fill = list(births = 0)) %>% 
  left_join(lookup, by = "OA11CD") %>% 
  
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, births)

sum(is.na(births_oa))
length(unique(births_oa$OA11CD))

#-------------------------------------------------------------------------------

births_2002_2010 <- filter(births_oa, year < 2011)
births_2011_2020 <- filter(births_oa, year >= 2011)

#---

source("functions/split_full_EW_into_separate_files.R")

.split_oas_into_year_files(births_2011_2020,
                           "processed/births_by_year",
                           "oa_births_all_EW")

#---

.split_oas_into_year_files(births_2002_2010,
                           "processed/births_by_year",
                           "oa_births_all_EW",
                           years = 2002:2010)

#---

