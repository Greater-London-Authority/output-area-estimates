library(ipfp)
library(mipfp)
library(reshape2)
library(dplyr)
library(tidyr)

.fiting_oa_deaths <- function(i_lsoa, c_sex, c_year,
                            lsoa_seed_df, lsoa_deaths_sya, deaths_oa,
                            target_dimensions_list){
  
  source("functions/fn_convert_df.R")
  #browser()
  # lsoa_seed_df <- data.frame(lsoa_seed_df) %>% arrange(gss_code, LSOA11CD, OA11CD, year, sex, age)
  # lsoa_deaths_sya <- data.frame(lsoa_deaths_sya) %>% arrange(gss_code, LSOA11CD, year, sex, age)
  # deaths_oa <- data.frame(deaths_oa) %>% arrange(gss_code, LSOA11CD, OA11CD, year, sex)
  
  full_out_df <- lsoa_seed_df[0,] %>% 
    select(gss_code, LSOA11CD, OA11CD, year, sex, age, value) %>% 
    data.frame()
  
  for(i_sex in c_sex){
    for(i_year in c_year){
      #browser()
      seed_df <- lsoa_seed_df %>%
        filter(LSOA11CD == i_lsoa, sex == i_sex, year == i_year) %>%
        arrange(OA11CD, age)
      
      seed_table <- .convert_df(seed_df, "gss_code ~ LSOA11CD ~ OA11CD ~ year ~ sex ~ age")
      
      tgt_lsoa_df <- lsoa_deaths_sya %>%
        filter(LSOA11CD == i_lsoa, sex == i_sex, year == i_year) %>%
        arrange(age)
      
      tgt_oa_df <- deaths_oa %>%
        filter(LSOA11CD == i_lsoa, sex == i_sex, year == i_year) %>%
        arrange(OA11CD)
      
      # add rows for any OAs missing from deaths data
      seed_oas <- unique(seed_df$OA11CD)
      
      tgt_oa_df <- tidyr::complete(tgt_oa_df, OA11CD = seed_oas, LSOA11CD, gss_code, sex, year, fill = list(value = 0))
      
      
      # deal with issues caused by inconsistencies between the LSOA and OA data
      
      total_lsoa_deaths <- sum(tgt_lsoa_df$value)
      total_oa_deaths <- sum(tgt_oa_df$value)
      
      if(total_lsoa_deaths == 0 | total_oa_deaths == 0){
        
        tgt_lsoa_df_scaled <- mutate(tgt_lsoa_df, value = 0)
        tgt_oa_df_scaled <- mutate(tgt_oa_df, value = 0)
        
      } else {
        
        tgt_oa_df_scaled <- mutate(tgt_oa_df, value = value * total_lsoa_deaths/total_oa_deaths)
        tgt_lsoa_df_scaled <- tgt_lsoa_df
      }
      
      # create target inputs from target data frames
      
      tgt_lsoa <- .convert_df(tgt_lsoa_df_scaled, "gss_code ~ LSOA11CD ~ year ~ sex ~ age")
      tgt_oa <- .convert_df(tgt_oa_df_scaled, "gss_code ~ LSOA11CD ~ OA11CD ~ year ~ sex")
      
      target_data <- list("lsoa" = tgt_lsoa,
                          "oa" = tgt_oa)
      #browser()
      results.ipfp <- Estimate(seed = seed_table,
                               target.list = target_dimension_list,
                               target.data = target_data,
                               method = "ipfp",
                               iter = 2000,
                               print = FALSE)
      
      output <- melt(results.ipfp$x.hat, value.name = "value") %>%
        setnames(c("gss_code", "LSOA11CD", "OA11CD","year", "sex", "age", "value")) %>%  
        mutate(gss_code = as.character(gss_code),
               LSOA11CD = as.character(LSOA11CD),
               OA11CD = as.character(OA11CD),
               year = as.integer(year),
               sex = as.character(sex),
               age = as.integer(age),
               value = as.numeric(value)) 
      
      full_out_df <- bind_rows(full_out_df, output) %>% data.frame()
    }
  }
  
  
  return(full_out_df)
}
