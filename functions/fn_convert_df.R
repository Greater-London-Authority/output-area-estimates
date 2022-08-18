library(reshape2)

.convert_df <- function(in_df, use_formula, fun.aggregate = NULL){
  
  out_table <- acast(data = in_df,
                     formula = as.formula(use_formula),
                     value.var = "value",
                     fill = 0,
                     fun.aggregate = fun.aggregate)
  
  
  return(out_table)
}