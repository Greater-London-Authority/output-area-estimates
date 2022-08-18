.lsoas_into_groups <- function(n_groups, all_lsoas){
  lsoa_groups <- list()
  lsoas_per_group <- floor(length(all_lsoas)/n_groups)
  for(i in 1:n_groups){
    a <- (lsoas_per_group*(i-1))+1
    b <- ifelse(i == n_groups, length(all_lsoas), lsoas_per_group*i)
    lsoa_groups[[i]] <- all_lsoas[a:b]
  }
  message(paste(lsoas_per_group, "lsoas per group"))
  message(paste("Last group has", length(lsoa_groups[[i]]), "lsoas."))
  return(lsoa_groups)
}