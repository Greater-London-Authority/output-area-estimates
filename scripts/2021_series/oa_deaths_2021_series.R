library(dplyr)
library(tidyr)
library(data.table)

f_paths <- list(revised_series = "E:/project_folders/demography/ben/R_projects/create_modelled_backseries/outputs/modelled_backseries.rds",
                oa_deaths_2020 = "processed/2021_series/deaths_by_year/oa_deaths_all_EW_2020.rds",
                oa_deaths_2021 = "processed/2021_series/deaths_by_year/oa_deaths_all_EW_2021.rds",
                lsoa_deaths = "Q:/Teams/D&PA/Data/births_and_deaths/lsoa_births_by_aom_deaths_2001_2021/")

dir.create("processed/2021_series/deaths_by_year", showWarnings = FALSE)

#-------------------------------------------------------------------------------

#2011-2020 same as existing

for(yr in 2011:2020){
  
  file.copy(paste0("processed/deaths_by_year/oa_deaths_all_EW_", yr, ".rds"),
            paste0("processed/2021_series/deaths_by_year/oa_deaths_all_EW_", yr, ".rds"))
  
}

#-------------------------------------------------------------------------------

# 2021

##LSOA DEATHS X age band X sex

lsoa_males_raw <- fread(paste0(f_paths$lsoa_deaths,"deathsbylsoamidyear01to21_males.csv")) %>% 
  filter(year == 2021) %>% 
  pivot_longer(cols = starts_with("Males"), names_to = "var", values_to = "lsoa_deaths") %>% 
  mutate(sex = "male",
         age_band = case_when(grepl("under",var)~"0",
                              grepl("over",var)~"85",
                              TRUE ~ substr(var,7,14)),
         band_start = case_when(grepl("under",var)~0,
                                grepl("over",var)~85,
                                TRUE ~ as.numeric(substr(var,7,8))),
         band_end = case_when(grepl("under",var)~0,
                              grepl("over",var)~90,
                              band_start == 1~4,
                              TRUE ~ band_start+4)) %>% 
  select(-LAD_name, -LSOA11NM, -var) %>%
  data.frame()

lsoa_females_raw <- fread(paste0(f_paths$lsoa_deaths,"deathsbylsoamidyear01to21_females.csv")) %>% 
  filter(year == 2021) %>% 
  pivot_longer(cols = starts_with("Females"), names_to = "var", values_to = "lsoa_deaths") %>% 
  mutate(sex = "female",
         age_band = case_when(grepl("under",var)~"0",
                              grepl("over",var)~"85",
                              TRUE ~ substr(var,9,16)),
         band_start = case_when(grepl("under",var)~0,
                                grepl("over",var)~85,
                                TRUE ~ as.numeric(substr(var,9,10))),
         band_end = case_when(grepl("under",var)~0,
                              grepl("over",var)~90,
                              band_start == 1~4,
                              TRUE ~ band_start+4)) %>% 
  select(-LAD_name, -LSOA11NM, -var) %>%
  data.frame()

lsoa_deaths <- bind_rows(lsoa_males_raw, lsoa_females_raw) %>% 
  rename(LAD21CD = LAD22CD)

sum(is.na(lsoa_deaths))

rm(lsoa_males_raw, lsoa_females_raw)

#---

#LA Deaths X SYA

LA_in <- readRDS(f_paths$revised_series)

LA_deaths <- LA_in %>% 
  rename(la_deaths = value,
         LAD21CD = gss_code) %>% 
  filter(component == "deaths", year == 2020) %>% 
  filter(substr(LAD21CD,2,3) %in% c("06","07","08","09")) %>% 
  select(LAD21CD, sex, age, la_deaths) %>% 
  mutate(la_deaths = ifelse(la_deaths < 0, 0 , la_deaths))

#---

#OA deaths mid-2020

OA_deaths_2020 <- readRDS(f_paths$oa_deaths_2020) %>% 
  select(-year)

#---

lsoa_sya_deaths_list <- list()

for(band in unique(lsoa_deaths$age_band)){
  
  x <- filter(lsoa_deaths, age_band == band)
  y <- filter(LA_deaths, age %in% unique(x$band_start):unique(x$band_end))
  
  lsoa_sya_deaths_list[[band]] <- popmodules::distribute_within_age_band(x,
                                                                         y,
                                                                         popn_1_col = "lsoa_deaths",
                                                                         popn_2_col = "la_deaths",
                                                                         min_age = unique(x$band_start),
                                                                         max_age = unique(x$band_end),
                                                                         col_aggregation = c("LAD21CD","sex"))
  
}

lsoa_sya_deaths <- bind_rows(lsoa_sya_deaths_list) %>% 
  data.frame() %>% 
  #arrange(LSOA11CD, sex, age) %>% # Unnecessary and long
  select(LSOA11CD, sex, age, lsoa_deaths)

rm(x,y,band)

sum(lsoa_sya_deaths$lsoa_deaths)
sum(lsoa_deaths$lsoa_deaths)

lsoa_deaths_total <- lsoa_sya_deaths %>%
  group_by(LSOA11CD) %>%
  summarise(lsoa_deaths = sum(lsoa_deaths),
            .groups = 'drop_last') %>%
  data.frame()

#Scale 2020 OA deaths to match 2021 LSOA deaths (sex and age)

OA_deaths_2021 <- OA_deaths_2020 %>% 
  group_by(LSOA11CD, sex, age) %>% 
  mutate(LSOA_total_deaths = sum(deaths)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(LSOA_total_deaths == 0, 0, deaths/LSOA_total_deaths)) %>% 
  select(-deaths) %>% 
  left_join(lsoa_sya_deaths, by = c("LSOA11CD", "sex", "age")) %>% 
  mutate(deaths = lsoa_deaths * proportion,
         year = 2021) %>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, deaths) 

sum(OA_deaths_2021$deaths)
sum(lsoa_deaths$lsoa_deaths)

#Scale again at the total deaths level (LSOA)

deaths_scaled_lsoa <- OA_deaths_2021 %>% 
  group_by(LSOA11CD) %>% 
  mutate(lsoa_total_deaths = sum(deaths)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(lsoa_total_deaths == 0, 0, deaths/lsoa_total_deaths)) %>% 
  select(-deaths) %>% 
  left_join(lsoa_deaths_total, by = "LSOA11CD") %>% 
  mutate(deaths = lsoa_deaths * proportion)%>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, deaths) 

sum(lsoa_deaths$lsoa_deaths)
sum(deaths_scaled_lsoa$deaths)
sum(is.na(deaths_scaled_lsoa))

#Scale again at the total deaths level (LAD)

lad_deaths_total <- lsoa_deaths %>% 
  group_by(LAD21CD) %>% 
  summarise(lad_deaths = sum(lsoa_deaths),
            .groups = 'drop_last') %>% 
  data.frame()

deaths_scaled_lad <- deaths_scaled_lsoa %>% 
  group_by(LAD21CD) %>% 
  mutate(lad_total_deaths = sum(deaths)) %>% 
  data.frame() %>% 
  mutate(proportion = ifelse(lad_total_deaths == 0, 0, deaths/lad_total_deaths)) %>% 
  select(-deaths) %>% 
  left_join(lad_deaths_total, by = "LAD21CD") %>% 
  mutate(deaths = lad_deaths * proportion)%>% 
  select(OA11CD, LSOA11CD, LAD21CD, year, sex, age, deaths) 

sum(lsoa_deaths$lsoa_deaths)
sum(deaths_scaled_lad$deaths)
sum(is.na(deaths_scaled_lsoa))

#-------------------------------------------------------------------------------

saveRDS(deaths_scaled_lad, f_paths$oa_deaths_2021)
