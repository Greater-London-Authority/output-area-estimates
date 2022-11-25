library(dplyr)

#2011-2020 same as existing

dir.create("processed/2021_series/births_by_year", showWarnings = FALSE)

for(yr in 2011:2020){
  
  readRDS(paste0("processed/births_by_year/oa_births_all_EW_", yr, ".rds")) %>% 
    filter(substr(OA11CD,1,1)=="E") %>% 
    saveRDS(paste0("processed/2021_series/births_by_year/oa_births_all_EW_", yr, ".rds"))
  
}

#-------------------------------------------------------------------------------

#2021

#Proportion of LA 0-year-olds in 2021 in each OA
#Apply to LA births

oa_pop <- readRDS("processed/2021_series/population_by_year/oa_population_all_EW_2021.rds")

LA_in <- readRDS("Q:/Teams/D&PA/Demography/MYE/gla_revised_mye_series.rds")

LA_births <- LA_in %>% 
  rename(la_births = value,
         LAD21CD = gss_code) %>% 
  filter(component == "births", year == 2021) %>% # TODO change to 2021
  filter(substr(LAD21CD,2,3) %in% c("06","07","08","09")) %>% 
  select(LAD21CD, sex, age, la_births)

births_2021 <- oa_pop %>% 
  filter(age == 0) %>% 
  group_by(LAD21CD, sex) %>% 
  mutate(LA_total_pop = sum(popn)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(LA_total_pop == 0, 0, popn/LA_total_pop)) %>% 
  left_join(LA_births, by = c("LAD21CD", "sex", "age")) %>% 
  mutate(births = la_births * proportion) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, births)

sum(is.na(births_2021))
sum(births_2021$births)
sum(LA_births$la_births)         

saveRDS(births_2021, paste0("processed/2021_series/births_by_year/oa_births_all_EW_2021.rds"))
