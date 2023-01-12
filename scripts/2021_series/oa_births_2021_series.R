library(dplyr)
library(data.table)

#2011-2020 same as existing

dir.create("processed/2021_series/births_by_year", showWarnings = FALSE)

for(yr in 2011:2020){
  
  readRDS(paste0("processed/births_by_year/oa_births_all_EW_", yr, ".rds")) %>% 
    #filter(substr(OA11CD,1,1)=="E") %>% 
    saveRDS(paste0("processed/2021_series/births_by_year/oa_births_all_EW_", yr, ".rds"))
  
}

#-------------------------------------------------------------------------------

#2021

#Proportion of LSOA 0-year-olds in 2021 in each OA
#Apply to LA births

oa_pop <- readRDS("processed/2021_series/population_by_year/oa_population_all_EW_2021.rds")

#LA_in <- readRDS("E:/project_folders/demography/ben/R_projects/create_modelled_backseries/outputs/modelled_backseries.rds")

#---

lsoa_births_path <- "Q:/Teams/D&PA/Data/births_and_deaths/lsoa_births_by_aom_deaths_2001_2021/"

lsoa_males_raw <- fread(paste0(lsoa_births_path,"birthsbylsoamidyear01to21_males.csv")) %>% 
  filter(year == 2021) %>% 
  mutate(sex = "male",
         age = 0,
         lsoa_births =  mother_24_under+mother_25_34+mother_35_older) %>% 
  select(LSOA11CD, sex, age, lsoa_births) %>%
  data.frame()

lsoa_females_raw <- fread(paste0(lsoa_births_path,"birthsbylsoamidyear01to21_females.csv")) %>% 
  filter(year == 2021) %>% 
  mutate(sex = "female",
         age = 0,
         lsoa_births =  mother_24_under+mother_25_34+mother_35_older) %>% 
  select(LSOA11CD, sex, age, lsoa_births) %>%
  data.frame()

lsoa_births <- bind_rows(lsoa_males_raw, lsoa_females_raw) #%>% 
  #filter(grepl("E01", LSOA11CD)) # TODO WALES

rm(lsoa_births_path, lsoa_males_raw, lsoa_females_raw)

#---

births_2021 <- oa_pop %>% 
  filter(age == 0) %>% 
  group_by(LSOA11CD, LAD21CD, sex) %>% 
  mutate(lsoa_total_pop = sum(popn)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(lsoa_total_pop == 0, 0, popn/lsoa_total_pop)) %>% 
  left_join(lsoa_births, by = c("LSOA11CD", "sex", "age")) 
  
#In places where we can't use the proportion method
#simply divide by the number of OAs in the LSOA
births_2021_a <- filter(births_2021, lsoa_total_pop == 0) %>% 
  group_by(LSOA11CD, sex) %>% 
  mutate(n = n(),
         births = lsoa_births/n) %>% 
  data.frame() %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, births)

#In other places divide the number of births in the lsoa
#by the the proportion of 0-year-olds in the OA
births_2021_b <-  filter(births_2021, lsoa_total_pop != 0) %>% 
  mutate(births = lsoa_births * proportion) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, births)

births_2021 <- bind_rows(births_2021_a, births_2021_b)

sum(is.na(births_2021))
sum(births_2021$births)
sum(lsoa_births$lsoa_births)         

saveRDS(births_2021, paste0("processed/2021_series/births_by_year/oa_births_all_EW_2021.rds"))

