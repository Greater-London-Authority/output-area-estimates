library(tidyverse)
library(dtplyr)
library(data.table)
library(popmodules)

library(foreach)
library(parallel)
library(doParallel)

library(ipfp)
library(mipfp)
library(reshape2)

source("functions/fn_age_groupings.R")
source("functions/fn_fitting_loop_lsoa_deaths.R")
source("functions/fn_split_data.R")

#### Create deaths x year x LSOA x sex x age_group x age 

demog_data <- "E:/project_folders/demography/demog_data/"

fpaths <- list(mye_coc = "Q:/Teams/D&PA/Data/population_estimates/ons_mid_year_estimates/current_series/mye_2020/mye_2020_components_EW_(2021_geog).rds",
              lsoa_deaths = paste0(demog_data, "deaths/deaths_lsoa_age/processed/deathsbylsoamidyear01to20.rds"),
              lsoa_births = paste0(demog_data, "births/births_lsoa_age_of_mother/processed/birthsbylsoamidyear01to20.rds"),
              lsoa_population = paste0(demog_data, "small_area_population_estimates/processed/aggregated_data/population_LSOA11_full_series.rds")
)

first_year <- 2011
last_year <- 2020

dir.create("processed/lsoa_deaths/", showWarnings = FALSE)

#-------------------------------------------------------------------------------

lookup_lsoa_lad <- fread("lookups/OA11_LSOA11_MSOA11_LAD11.csv") %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, LAD21CD = LAD11CD) %>% 
  recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE, col_geog = "LAD21CD") %>% 
  select(LSOA11CD, gss_code = LAD21CD) %>%
  distinct()

#-------------------------------------------------------------------------------

### Target 1 - deaths x year x LAD x sex x age_group x age
deaths_lad <- readRDS(fpaths$mye_coc) %>%
  lazy_dt() %>% 
  filter(component == "deaths") %>%
  filter(year >= first_year) %>%
  select(-c(geography, country, component, gss_name)) %>%
  mutate(value = as.numeric(value)) %>%
  mutate(value = case_when(
    value < 0 ~ 0,
    TRUE ~ value
  )) %>%
  .add_age_group() %>% 
  data.frame()

### Target 2 - deaths x year x LAD x LSOA x sex x age_group
deaths_lsoa <- readRDS(fpaths$lsoa_deaths) %>%
  lazy_dt() %>% 
  select(year, LSOA11CD, sex, age_group, value = deaths) %>%
  filter(year >= first_year) %>%
  left_join(lookup_lsoa_lad, by = "LSOA11CD") %>% 
  data.frame()

### Seed - year x LAD x LSOA x sex x age_group x age
# use population at start of each annual period as seed. 
# need to add 1 to both year and age of population data and remove the final year
# births for same year used for age 0

seed_population <- list()

# births
seed_population[[1]] <- readRDS(fpaths$lsoa_births) %>%
  lazy_dt() %>% 
  group_by(year, LSOA11CD, sex) %>%
  summarise(value = sum(births), .groups = "drop") %>%
  mutate(age = -1,
         year = year - 1) %>% 
  data.frame() %>%
  select(year, LSOA11CD, sex, age, value) 

# population  
seed_population[[2]] <- readRDS(fpaths$lsoa_population) %>%
  data.frame() %>% 
  select(year, LSOA11CD, sex, age, value = population) 

#-------------------------------------------------------------------------------

full_seed_df <- list()

# age on
full_seed_df[[1]]  <- rbindlist(seed_population) %>% 
  data.frame() %>% 
  mutate(year = year + 1,
         age = age + 1) %>%
  filter(between(year, first_year, last_year))

# group over 90
full_seed_df[[2]] <- full_seed_df[[1]] %>% 
  filter(age >= 90)  %>%
  group_by(LSOA11CD, year, sex) %>%
  summarise(value = sum(value), .groups = "drop") %>%
  mutate(age = 90) %>%
  select(year, LSOA11CD, sex, age, value) %>% 
  data.frame()

full_seed_df[[1]] <- full_seed_df[[1]] %>% 
  filter(age < 90)  %>%
  data.frame()

# bind and add age group
full_seed_df <- rbindlist(full_seed_df) %>% 
  .add_age_group() %>%
  left_join(lookup_lsoa_lad, by = "LSOA11CD") %>% 
  data.frame()

#-------

rm(seed_population, lookup_lsoa_lad)
gc()

#-------------------------------------------------------------------------------

#### dimension order: 
#1 gss_code
#2 LSOA11CD
#3 year
#4 sex
#5 age_group
#6 age

target_dimension_list <- list("lad" = c(1, 3, 4, 5, 6),
                              "lsoa" = c(1, 2, 3, 4, 5))

c_lad <- unique(full_seed_df$gss_code)
c_sex <- unique(full_seed_df$sex)
c_year <- unique(full_seed_df$year)
c_age_group <- unique(full_seed_df$age_group)

#----------------------

#TEMP
#c_lad <- c_lad[1:10]
# c_sex <- "female"
# c_year <- 2011
# c_age_group <- "1_4"
# i_lad <- "E09000002"

n_cores <- 10
lad_groups <- list()
lads_per_group <- floor(length(c_lad)/n_cores)

for(i in 1:n_cores){
  a <- (lads_per_group*(i-1))+1
  b <- ifelse(i == n_cores, length(c_lad), lads_per_group*i)
  lad_groups[[i]] <- data.frame(gss_code = c_lad[a:b], group =i)
}

#------------------------

full_seed_list <- full_seed_df %>%
  left_join(rbindlist(lad_groups), by = "gss_code") %>% 
  .split_data3("group")

deaths_lad_list <- deaths_lad %>%
  left_join(rbindlist(lad_groups), by = "gss_code") %>% 
  .split_data3("group")

deaths_lsoa_list <- deaths_lsoa %>%
  left_join(rbindlist(lad_groups), by = "gss_code") %>% 
  .split_data3("group")

rm(full_seed_df, deaths_lad, deaths_lsoa)
gc()

#------------------------

cl <- makeCluster(n_cores)
registerDoParallel(cl)

for(i_year in c_year){
  
  filtered_seed_yr <- lapply(full_seed_list, function(x) filter(x, year == i_year))
  filtered_lad_yr <- lapply(deaths_lad_list, function(x) filter(x, year == i_year))
  filtered_lsoa_yr <- lapply(deaths_lsoa_list, function(x) filter(x, year == i_year))
  
  #----
  
  lsoa_deaths_sya <- foreach(i = 1:n_cores, .packages=c("tidyverse","data.table","ipfp","mipfp","reshape2"),
                             .combine = rbind, .multicombine = TRUE) %dopar% {
                               
                               output <- list()
                               
                               filtered_seed <- filtered_seed_yr[[i]]
                               filtered_lad <- filtered_lad_yr[[i]]
                               filtered_lsoa <- filtered_lsoa_yr[[i]]
                               
                               for(i_lad in lad_groups[[i]]$gss_code) {
                                 
                                 output[[i_lad]] <- .fiting_loop_lsoa(i_lad, c_sex, i_year, c_age_group,
                                                                      filter(filtered_seed, gss_code == i_lad),
                                                                      filter(filtered_lad, gss_code == i_lad),
                                                                      filter(filtered_lsoa, gss_code == i_lad),
                                                                      target_dimension_list)
                                 
                               }
                               
                               rbindlist(output)
                             }
  
  #----
  
  lsoa_deaths_sya <- lsoa_deaths_sya %>% 
    rename(LAD21CD = gss_code)
  
  saveRDS(lsoa_deaths_sya, paste0("processed/lsoa_deaths/lsoa_deaths_all_EW_",i_year,"_sya.rds"))
 
}




