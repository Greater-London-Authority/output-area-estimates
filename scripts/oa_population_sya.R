#Process small area estimate Excels into 1 rds dataframe
#Doesn't scale nicely for parallel processing

library(dplyr)
library(tidyr)
library(data.table)
library(readxl)
library(readr)
library(stringr)
library(popmodules)
library(dtplyr)


raw_oa <- "Q:/Teams/D&PA/Data/population_estimates/ons_small_area_population_estimates/Small Area Ests/OA"

gla_mye <- "Q:/Teams/D&PA/Data/population_estimates/gla_adjusted_mid_year_estimates/2020/population_gla.rds"

lookup <- fread("lookups/OA11_LSOA11_MSOA11_LAD11.csv") %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, LAD21CD = LAD11CD) %>% 
  recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE, col_geog = "LAD21CD")

dir.create("processed/population_by_year", showWarnings = FALSE)
dir.create("processed/population_by_region", showWarnings = FALSE)

#-------------------------------------------------------------------------------

#get data for 2012 to 2020

regions <- list.dirs(raw_oa, recursive = FALSE)
oa_estimates <- list()

for(j in 1:10){
  
  region_dir <- regions[j]
  nm <- substr(region_dir, 96, nchar(region_dir))
  oa_data <- list()
  message(nm)
  
  for(i in 2013:2020){
    
    p <- paste0(region_dir, "/" ,nm, "_oa_mid-",i, ".xlsx")
    
    oa_data[[i]] <- bind_rows(
      
      read_excel(p, sheet = 5, skip = 3) %>%
        select(-c('All Ages')) %>%
        pivot_longer(cols = -c(OA11CD, LSOA11CD), names_to = "age", values_to = "popn") %>%
        mutate(sex = "male") %>%
        mutate(year = i,
               age = as.numeric(str_extract(age, "[0-9]+"))),
      
      read_excel(p, sheet = 6, skip = 3) %>%
        select(-c('All Ages')) %>%
        pivot_longer(cols = -c(OA11CD, LSOA11CD), names_to = "age", values_to = "popn") %>%
        mutate(sex = "female") %>%
        mutate(year = i,
               age = as.numeric(str_extract(age, "[0-9]+")))
    )
  }
  
  #get data prior to 2012
  
  for(i in 2002:2012){
    
    p <- paste0(region_dir, "/", nm, "_oa_2002-2012.xls")
    if(nm=="london"){p <- paste0(p,"x")}
    
    oa_data[[i]] <- read_excel(p, sheet = i-2001) %>%
      select(-c('All Ages', LAD11CD)) %>%
      pivot_longer(cols = -c(OA11CD, Sex), names_to = "age", values_to = "popn") %>%
      mutate(sex = recode(Sex, '1' = "male", '2' = "female")) %>%
      mutate(year = i,
             age = as.numeric(str_extract(age, "[0-9]+"))) %>%
      select(-Sex) %>%
      left_join(lookup, by = "OA11CD") %>% 
      select(-LAD21CD)
  }
  
  
  oa_estimates[[j]] <- bind_rows(oa_data) %>% 
    left_join(lookup, by = c("OA11CD","LSOA11CD")) %>% 
    select(OA11CD, LSOA11CD, LAD21CD, year, age, sex, popn) %>% 
    data.frame()
  
}

rm(oa_data)
oa_estimates <- bind_rows(oa_estimates)
sum(is.na(oa_estimates))


oa_estimates_2002_2010 <- filter(oa_estimates, year < 2011)
oa_estimates_2011_2020 <- filter(oa_estimates, year >= 2011)
rm(oa_estimates)
gc()


#-------------------------------------------------------------------------------

source("functions/split_full_EW_into_separate_files.R")

.split_oas_into_region_files(oa_estimates_2011_2020,
                            "processed/population_by_region",
                            "oa_population_ONS_series_2011-2020")

.split_oas_into_year_files(oa_estimates_2011_2020,
                          "processed/population_by_year",
                          "oa_population_all_EW_ONS_series")


#------------------------

.split_oas_into_region_files(oa_estimates_2002_2010,
                            "processed/population_by_region",
                            "oa_population_2002-2010")

.split_oas_into_year_files(oa_estimates_2002_2010,
                          "processed/population_by_year",
                          "oa_population_all_EW",
                          years = 2002:2010)

rm(oa_estimates_2002_2010)
gc()

#-------------------------------------------------------------------------------

# constraint to GLA series

gla_series <- readRDS(gla_mye) %>% 
  rename(LAD21CD = gss_code,
         LAD_pop = popn)

oa_scaling <- oa_estimates_2011_2020 %>% 
  lazy_dt() %>% 
  group_by(LAD21CD, age, sex, year) %>% 
  summarise(OA_popn = sum(popn), .groups = 'drop_last') %>% 
  data.frame() %>% 
  left_join(gla_series, by = c("LAD21CD", "age", "sex", "year")) %>% 
  mutate(scaling_factor = ifelse(OA_popn == 0, 0, LAD_pop / OA_popn)) %>% 
  select(LAD21CD, year, sex, age, scaling_factor)

oa_constrained <- oa_estimates_2011_2020 %>% 
  lazy_dt() %>% 
  left_join(oa_scaling, by=c("LAD21CD","year","sex","age")) %>% 
  mutate(scaled_popn = popn * scaling_factor) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, age, sex, popn = scaled_popn) %>% 
  data.frame()

saveRDS(oa_constrained, "processed/oa_population_all_EW_2011-2020_GLA_series.rds")

#-------------------------------------------------------------------------------

.split_oas_into_region_files(oa_constrained,
                            "processed/population_by_region",
                            "oa_population_GLA_series_2011-2020")

.split_oas_into_year_files(oa_constrained,
                          "processed/population_by_year",
                          "oa_population_all_EW_GLA_series")

#-------------------------------------------------------------------------------

rm(list = ls())
gc()

