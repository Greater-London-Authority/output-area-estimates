
.fiting_loop_lsoa <- function(i_lad, c_sex, c_year, c_age_group,
                              full_seed_df, deaths_lad, deaths_lsoa,
                              target_dimension_list){
  
  source("functions/fn_convert_df.R")
  
  full_seed_df <- data.frame(full_seed_df)
  deaths_lad <- data.frame(deaths_lad)
  deaths_lsoa <- data.frame(deaths_lsoa)
  
  full_out_df <- full_seed_df[0,] %>% 
    select(gss_code, LSOA11CD, year, sex, age_group, age, value) %>% 
    data.frame()
  
  for(i_sex in c_sex){
    for(i_year in c_year){
      for(i_age_group in c_age_group){
        
       # if(i_age_group=="80_84"){browser()}
        
        seed_df <- full_seed_df %>%
          filter(gss_code == i_lad, sex == i_sex, year == i_year, age_group == i_age_group) %>%
          arrange(LSOA11CD, age_group, age)
        
        seed_table <- .convert_df(seed_df, "gss_code ~ LSOA11CD ~ year ~ sex ~ age_group ~ age")
        
        tgt_lad_df <- deaths_lad %>%
          filter(gss_code == i_lad, sex == i_sex, year == i_year, age_group == i_age_group) %>%
          arrange(age_group, age)
        
        tgt_lsoa_df <- deaths_lsoa %>%
          filter(gss_code == i_lad, sex == i_sex, year == i_year,  age_group == i_age_group) %>%
          arrange(LSOA11CD, age_group)
        
        # deal with issues caused by inconsistencies between the LAD and LSOA data
        
        total_lad_deaths <- sum(tgt_lad_df$value)
        total_lsoa_deaths <- sum(tgt_lsoa_df$value)
        
        if(total_lad_deaths == 0 | total_lsoa_deaths == 0){
          
          tgt_lsoa_df_scaled <- mutate(tgt_lsoa_df, value = 0)
          tgt_lad_df_scaled <- mutate(tgt_lad_df, value = 0)
          
        } else {
          
          tgt_lsoa_df_scaled <- mutate(tgt_lsoa_df, value = value * total_lad_deaths/total_lsoa_deaths)
          tgt_lad_df_scaled <- tgt_lad_df
        }
        
        # create target inputs from target data frames
        
        tgt_lad <- .convert_df(tgt_lad_df_scaled, "gss_code ~ year ~ sex ~ age_group ~ age")
        tgt_lsoa <- .convert_df(tgt_lsoa_df_scaled, "gss_code ~ LSOA11CD ~ year ~ sex ~ age_group")
        
        target_data <- list("lad" = tgt_lad,
                            "lsoa" = tgt_lsoa)
        
        results.ipfp <- Estimate(seed = seed_table,
                                 target.list = target_dimension_list,
                                 target.data = target_data,
                                 method = "ipfp",
                                 iter = 2000,
                                 print = FALSE)
        
        output <- melt(results.ipfp$x.hat, value.name = "value") %>%
          setnames(c("gss_code", "LSOA11CD", "year", "sex", "age_group", "age", "value")) %>%  
          mutate(gss_code = as.character(gss_code),
                 LSOA11CD = as.character(LSOA11CD),
                 year = as.integer(year),
                 sex = as.character(sex),
                 age_group  = as.character(age_group),
                 age = as.integer(age),
                 value = as.numeric(value)) %>% 
          data.frame()
        
        #browser()
        full_out_df <- bind_rows(full_out_df, output) %>% data.frame()
         
      }
    }
  }
  
  return(full_out_df)
}