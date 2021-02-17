pull_column <- function(filepath, col_id){
  read_csv(filepath) %>% pull(col_id) %>% unique()
}
