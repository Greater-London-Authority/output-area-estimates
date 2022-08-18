library(dplyr)

.split_sya <- function(in_df) {
  
  out_df <- in_df %>%
    mutate(start_age = str_extract(age_group, "[0-9]+")) %>%
    mutate(end_age = str_extract(age_group, "[0-9]+$")) %>%
    mutate(end_age = case_when(
      is.na(end_age) ~ "90",
      TRUE ~ end_age
    )) %>%
    mutate(start_age = as.integer(start_age), end_age = as.integer(end_age)) %>%
    mutate(num_years = 1 + end_age - start_age) %>%
    mutate(value = deaths/num_years) %>%
    select(-c(start_age, end_age, num_years, deaths))
  
  return(out_df)
}

.add_age_group <- function(in_df) {
  
  out_df <- in_df %>%
    mutate(age_group = case_when(
      age <= 0 ~ "0",
      age <= 4 ~ "1_4",
      age <= 9 ~ "5_9",
      age <= 14 ~ "10_14",
      age <= 19 ~ "15_19",
      age <= 24 ~ "20_24",
      age <= 29 ~ "25_29",
      age <= 34 ~ "30_34",
      age <= 39 ~ "35_39",
      age <= 44 ~ "40_44",
      age <= 49 ~ "45_49",
      age <= 54 ~ "50_54",
      age <= 59 ~ "55_59",
      age <= 64 ~ "60_64",
      age <= 69 ~ "65_69",
      age <= 74 ~ "70_74",
      age <= 79 ~ "75_79",
      age <= 84 ~ "80_84",
      TRUE ~ "85+"
    ))
  
  return(out_df)
}
