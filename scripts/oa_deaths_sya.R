#### Create Output Area deaths x year x sex x age 

library(tidyverse)
library(dtplyr)
library(data.table)
library(popmodules)

source("functions/fn_fitting_oa_deaths.R")
source("functions/fn_split_data.R")
source("functions/fn_lsoas_into_groups.R")

#-------------------------------------------------------------------------------

dir.create("processed/temp_files", showWarnings = FALSE)

fpaths <- list(oa_births = "processed/births_by_year/oa_births_all_EW_",
               oa_deaths = "processed/deaths_by_year/oa_deaths_all_EW_2002-2020_totals.rds",
               oa_population = "processed/population_by_year/oa_population_all_EW_",
               lsoa_deaths = "processed/lsoa_deaths/lsoa_deaths_all_EW_",
               output = "processed/deaths_by_year/",
               temp = "processed/temp_files/"
)

first_year <- 2011
last_year <- 2020

dir.create(fpaths$output, showWarnings = FALSE)
dir.create(fpaths$temp, showWarnings = FALSE)

#-------------------------------------------------------------------------------

lookup <- fread("lookups/OA11_LSOA11_MSOA11_LAD11.csv") %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, gss_code = LAD11CD) %>% 
  recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE)

#-------------------------------------------------------------------------------

### Target 2 - deaths x gss_code x LSOA x OA x year x sex 
deaths_oa <- readRDS(fpaths$oa_deaths) %>%
  lazy_dt() %>%
  filter(year >= first_year) %>%
  rename(value = deaths,
         gss_code = LAD21CD) %>% 
  data.frame() %>% 
  select(gss_code, LSOA11CD, OA11CD, year, sex, value)

# Convert to a list where each element is an LSOA
deaths_oa_list <- .split_data3(deaths_oa, "LSOA11CD")

rm(deaths_oa)
gc()

#-------------------------------------------------------------------------------

# Do each year separately
for(yr in first_year:last_year){
  
  ### Seed - year x LAD x LSOA x OA x sex x age_group x age
  # use population at start of each annual period as seed. 
  # need to add 1 to both year and age of population data and remove the final year
  # births for same year used for age 0
  
  seed_population <- list()
  
  if(yr <= 2011){
    seed_population[[1]] <- readRDS(paste0(fpaths$oa_pop, yr-1, ".rds")) %>%
      rename(value = popn) %>% 
      select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, value)
  } else {
    seed_population[[1]] <- readRDS(paste0(fpaths$oa_pop, "GLA_series_", yr-1, ".rds")) %>%
      rename(value = popn)%>% 
      select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, value)
  }
  
  seed_population[[2]] <- readRDS(paste0(fpaths$oa_births, yr, ".rds")) %>% 
    rename(value = births) %>%
    mutate(year = year - 1,
           age = age - 1)%>% 
    select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, value)
  
  #-------------------------------------------------------------------------------
  
  ### Target 1 - deaths x gss_code x LSOA x year x sex x age [output from stage 1]
  lsoa_deaths_sya <- readRDS(paste0(fpaths$lsoa_deaths, yr, "_sya.rds")) %>%
    lazy_dt() %>% 
    rename(gss_code = LAD21CD) %>% 
    filter(year >= first_year) %>% 
    select(gss_code, LSOA11CD, year, sex, age, value)
  
  # Convert to a list where each element is an LSOA
  deaths_lsoa_list <- .split_data3(lsoa_deaths_sya, "LSOA11CD")
  
  rm(lsoa_deaths_sya)
  gc()
  
  #-------------------------------------------------------------------------------
  
  # Births are normally added after the population has been aged on.
  # An alternate method, used here, is to start with births and
  # subtract 1 year and make the age -1. Then population is added and everything
  # moved forward a year and an age. 
  
  #-------------------------------------------------------------------------------
  
  full_seed_df <- list()
  
  full_seed_df[[1]] <- rbindlist(seed_population) %>% 
    data.frame() %>% 
    mutate(year = year + 1,
           age = age + 1)
  
  full_seed_df[[2]] <- full_seed_df[[1]] %>% 
    filter(age >= 90)  %>%
    group_by(OA11CD, LSOA11CD, LAD21CD, year, sex) %>%
    summarise(value = sum(value), .groups = "drop") %>% 
    mutate(age = 90) %>% 
    select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, value) %>%
    data.frame()
  
  full_seed_df[[1]] <- full_seed_df[[1]] %>% 
    filter(age < 90)  %>%
    data.frame()
  
  full_seed_df <- rbindlist(full_seed_df) %>% 
    data.frame() %>% 
    rename(gss_code = LAD21CD) %>% 
    select(gss_code, LSOA11CD, OA11CD, year, sex, age, value)
  
  rm(seed_population)
  gc()
  
  #-------------------------------------------------------------------------------
  
  # Convert to a list where each element is an LSOA
  full_seed_list <- .split_data3(full_seed_df, "LSOA11CD")
  
  rm(full_seed_df)
  gc()
  
  #-------------------------------------------------------------------------------
  
  # dimension order: 
  #1 gss_code
  #2 LSOA11CD
  #3 OA11CD
  #4 year
  #5 sex
  #6 age
  
  target_dimension_list <- list("lsoa" = c(1, 2, 4, 5, 6),
                                "oa" = c(1, 2, 3, 4, 5))
  
  c_lsoa <- unique(lookup$LSOA11CD)
  c_sex <- c("female","male")
  c_year <- yr 
  
  #A list of 35 element each containing 1000 LSOAs
  lsoa_groups <- .lsoas_into_groups(37, c_lsoa)
  
  #------------------------
  # The ipf process works best as an lapply. A for loop is slow and due to the size
  # of the base data a parallel loop isn't an option (the parallel process copies the 
  # global environment to each core and this quickly overwhelms memory and the process
  # aborts). 
  
  # lapply takes around 3 seconds per LSOA and 4 minutes per 1000 LSOAs
  # Each year loop takes ~2.5hrs
  # Total time of 24 hours (started 8:46)
  # Uses ~9% CPU and ~25% memory
  
  
  message("running fitting loop") 
  for(j in 1:length(lsoa_groups)){
    
    cat('\r', paste0(yr, ": ", j, " of ", length(lsoa_groups)))
    flush.console()
    
    output <- lapply(lsoa_groups[[j]],
                     function(i_lsoa){
                       .fiting_oa_deaths(
                         i_lsoa,
                         c_sex, c_year,
                         lsoa_seed_df = full_seed_list[[i_lsoa]],
                         lsoa_deaths_sya =  deaths_lsoa_list[[i_lsoa]],
                         deaths_oa = filter(deaths_oa_list[[i_lsoa]], year == yr),
                         target_dimensions_list)
                     }) %>% 
      rbindlist()
    
    saveRDS(output, paste0(fpaths$temp, "temp_oa_deaths_sya_", j, "_", yr, ".rds"))
    
    rm(output)
    gc()
    
  }
  
  oa_deaths_list <- list()
  for(i in 1:length(lsoa_groups)){
    oa_deaths_list[[i]] <- readRDS(paste0(fpaths$temp, "temp_oa_deaths_sya_", i, "_", yr, ".rds"))
  }
  
  oa_deaths_sya <- rbindlist(oa_deaths_list) %>% 
    rename(LAD21CD = gss_code,
           deaths = value) %>% 
    data.frame()
  
  saveRDS(oa_deaths_sya, paste0(fpaths$output, "oa_deaths_all_EW_", yr, ".rds"))
  
  rm(deaths_lsoa_list, deaths_oa_list, full_seed_list, target_dimension_list, lookup, j, i)
  gc()
  
}

#-------------------------------------------------------------------------------

rm(list = ls())
gc()
