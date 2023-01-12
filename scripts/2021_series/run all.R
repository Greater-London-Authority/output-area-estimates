a <- c("population", "births", "deaths", "net_migration")

for(i in a){
  
  source(paste0("oa_",i,"_2021_series.R"))
  
}