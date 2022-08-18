library(utils)
library(dtplyr)
library(dplyr)

.split_data <- function(x, split_on = "LSOA11CD"){
  
  message("converting to dataframe to list")
  all_areas <- unique(x[[split_on]])
  
  x <- lazy_dt(x) %>% mutate(filter_col__ = !!sym(split_on))
  y <- list()
  j <- 0
  
  message("running loop")
  for(i in all_areas){
    
    j <- j+1
    cat('\r', j)
    #cat('\r', (floor(j/100)*100))
    flush.console()
    
    y[[i]] <- filter(x, filter_col__ == i) %>%
      select(-filter_col__) %>%
      data.frame()
  }
  
  cat('\r', j)
  return(y)
}


.split_data2 <- function(data, split_on = "LSOA11CD"){
  
  message("converting to dataframe to list")
  all_areas <- unique(data[[split_on]])
  
  message("running lapply")
  data <- lazy_dt(data) %>% mutate(filter_col__ = !!sym(split_on))
  
  y <- all_areas %>%
    lapply(function(x){
      filter(data, filter_col__ == x) %>%
        select(-filter_col__) %>%
        data.frame()
    })
  
  return(y)
}


.split_data3 <- function(data, split_on = "LSOA11CD"){
  
  x <- data %>%
    data.frame() %>%
    group_by(!!sym(split_on)) %>%
    group_split()
  
  y <- character()
  for(a in 1:length(x)){
    y <- c(y, unique(x[[a]][[split_on]]))
  }
  
  names(x) <- y
  
  return(x)
  
}