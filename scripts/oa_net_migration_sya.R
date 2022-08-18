library(tidyverse)
library(popmodules)
library(dtplyr)
library(data.table)

source("functions/fn_split_data.R")

start_year <- 2011
final_year <- 2020

fpaths <- list(oa_pop  = "processed/population_by_year/oa_population_all_EW_",
               oa_births = "processed/births_by_year/oa_births_all_EW_",
               oa_deaths = "processed/deaths_by_year/oa_deaths_all_EW_",
               net_mig = "processed/net_migration_by_year/oa_net_migration_all_EW_")

dir.create("processed/net_migration_by_year", showWarnings = FALSE)

#-------------------------------------------------------------------------------

# Do each year separately because of the size of the data

for(yr in start_year:final_year){
  
  message(yr)
  
  if(yr == start_year){
    comp_pop <- readRDS(paste0(fpaths$oa_pop, yr-1, ".rds")) %>%
      rename(prev = popn)
  } else {
    comp_pop <- oa_pop %>% rename(prev = value) %>% data.frame()
  }
  
  oa_pop <- readRDS(paste0(fpaths$oa_pop, "GLA_series_", yr, ".rds")) %>%
    lazy_dt() %>% 
    rename(value = popn)
  
  oa_births <- readRDS(paste0(fpaths$oa_births, yr, ".rds")) 
  
  oa_deaths <- readRDS(paste0(fpaths$oa_deaths, yr, ".rds")) %>% 
    lazy_dt()
  
  #-----------------------------------------------------------------------------
  
  # Births are normally added after the population has been aged on.
  # An alternate method, used here, is to start with births and
  # subtract 1 year and make the age -1. Then population is added and everything
  # moved forward a year and an age. 

  comp_df <- oa_births %>%
    rename(prev = births) %>% 
    mutate(year = year - 1,
           age = age - 1) %>% 
    bind_rows(comp_pop) %>% 
    select(OA11CD, year, sex, age, prev)
  
  all_ages <- lazy_dt(comp_df) %>%
    mutate(year = year + 1) %>%
    mutate(age = ifelse(age == 90, 90, age + 1))
  
  last_group <- filter(all_ages, age == 90)  %>%
    group_by(OA11CD, year, sex, age) %>%
    summarise(prev = sum(prev), .groups = "drop") %>%
    data.frame()
  
  comp_df <- filter(all_ages, age != 90)  %>%
    data.frame() %>%
    bind_rows(last_group) %>%
    lazy_dt()
  
  rm(oa_births, last_group, all_ages)
  gc()
  
  #-----------------------------------------------------------------------------
  
  net_mig <- comp_df %>% 
    left_join(oa_pop, by = c("OA11CD", "year", "sex", "age")) %>% 
    left_join(oa_deaths, by = c("OA11CD", "LSOA11CD",  "LAD21CD", "year", "sex", "age")) %>%
    mutate(change = value - prev) %>%
    mutate(net_mig = change + deaths) %>%
    select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, net_mig) %>% 
    data.frame()
  
  #-----------------------------------------------------------------------------
  
  if(sum(is.na(net_mig))!=0){
    message(paste(sum(is.na(net_mig)), "NAs in year", yr))
    stop()
  }
  
  saveRDS(net_mig, paste0(fpaths$net_mig, yr, ".rds"))
  
  rm(net_mig, comp_pop, comp_df)
  gc()
  
}
