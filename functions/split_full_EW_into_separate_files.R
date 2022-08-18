library(dplyr)
library(dtplyr)
library(popmodules)
library(data.table)
library(stringr)

.split_oas_into_region_files <- function(input, output_folder, output_file_name){
  
  lookup <- .get_region_lookup()
  regions <- unique(lookup$gss_name)
  input <- lazy_dt(input)
  
  for(i in regions){
    
    message(i)
    
    dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)
    nm <- paste0(output_folder, "/", output_file_name, "_", i, ".rds")
    
    x <- unique(filter(lookup, gss_name == i)$OA11CD)
    
    y <- filter(input, OA11CD %in% x) %>% data.frame()
    
    saveRDS(y, nm)
    
  }
  
}

#------------------------

.split_oas_into_year_files <- function(input, output_folder, output_file_name,
                                       years = 2011:2020){
  
  input <- lazy_dt(input)
  dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)
  
  for(i in years){
    
    message(i)
    
    nm <- paste0(output_folder, "/", output_file_name, "_", i, ".rds")
    
    y <- filter(input, year == i) %>% data.frame()
    
    saveRDS(y, nm)
    
  }
  
}

#------------------------

.get_region_lookup <- function(){
  fread("lookups/OA11_LSOA11_MSOA11_LAD11.csv") %>% 
    data.frame() %>% 
    select(OA11CD, LSOA11CD, gss_code = LAD11CD) %>% 
    recode_gss_codes(recode_to_year = 2021, aggregate_data = FALSE) %>% 
    left_join(readRDS("lookups/district_to_region_(2021 geog).rds"), by = "gss_code") %>% 
    select(-gss_code) %>% 
    rename(gss_code = region_gss_code) %>% 
    left_join(readRDS("lookups/gss_code_to_name.rds"), by = "gss_code") %>% 
    mutate(gss_name = str_remove_all(gss_name, "\\ \\(total\\)"))
}


