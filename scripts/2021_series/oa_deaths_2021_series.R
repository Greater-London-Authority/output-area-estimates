library(dplyr)

#2011-2020 same as existing

dir.create("processed/2021_series/deaths_by_year", showWarnings = FALSE)

for(yr in 2011:2020){
  
  readRDS(paste0("processed/deaths_by_year/oa_deaths_all_EW_", yr, ".rds")) %>% 
    filter(substr(OA11CD,1,1)=="E") %>% 
    saveRDS(paste0("processed/2021_series/deaths_by_year/oa_deaths_all_EW_", yr, ".rds"))
  
}


# 2021

# constrain 2020 deaths to the 2021 total at LA

LA_in <- readRDS("Q:/Teams/D&PA/Demography/MYE/gla_revised_mye_series.rds")

LA_deaths <- LA_in %>% 
  rename(la_deaths = value,
         LAD21CD = gss_code) %>% 
  filter(component == "deaths", year == 2021) %>% 
  filter(substr(LAD21CD,2,3) %in% c("06","07","08","09")) %>% 
  select(LAD21CD, sex, age, la_deaths)

LA_deaths_total <- LA_deaths %>% 
  group_by(LAD21CD) %>% 
  summarise(la_deaths = sum(la_deaths),
            .groups = 'drop_last') %>% 
  data.frame()

deaths_2020 <- readRDS(paste0("processed/2021_series/deaths_by_year/oa_deaths_all_EW_2020.rds"))

#Scale 2020 OA deaths to match 2021 LA deaths (sex and age)
deaths_2021 <- deaths_2020 %>% 
  group_by(LAD21CD, sex, age) %>% 
  mutate(LA_total_deaths = sum(deaths)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(LA_total_deaths == 0, 0, deaths/LA_total_deaths)) %>% 
  select(-deaths) %>% 
  left_join(LA_deaths, by = c("LAD21CD", "sex", "age")) %>% 
  mutate(deaths = la_deaths * proportion) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, deaths) 
  
#Scale again at the total deaths level
deaths_2021_final <- deaths_2021 %>% 
  group_by(LAD21CD) %>% 
  mutate(LA_total_deaths = sum(deaths)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(LA_total_deaths == 0, 0, deaths/LA_total_deaths)) %>% 
  select(-deaths) %>% 
  left_join(LA_deaths_total, by = "LAD21CD") %>% 
  mutate(deaths = la_deaths * proportion) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, deaths) 

sum(LA_deaths$la_deaths)
sum(deaths_2021_final$deaths)
sum(is.na(deaths_2021))

saveRDS(deaths_2021_final, paste0("processed/2021_series/deaths_by_year/oa_deaths_all_EW_2021.rds"))
