get_ned_buffers <- function(buffer_rds, zoom, dummy){
  # turn bbox into sf, filter centroids in bbox, then use poly_sf as unit to fetch data
  buffered_sf <- buffer_rds %>% readRDS() %>% mutate(ID = row_number())
  suppressMessages(elevatr::get_elev_raster(buffered_sf, z = zoom, verbose = FALSE)) %>%
    raster::extract(y = buffered_sf, df = TRUE) %>% group_by(ID) %>%
    summarize_all(.funs = median, na.rm = TRUE, .groups = 'drop') %>%
    rename(elev_m = 2) %>% inner_join(buffered_sf, by = 'ID') %>%
    select(site_id, elev_m)

}

get_ned_points <- function(point_rds, zoom, dummy){

  point_rds %>% readRDS() %>%
    elevatr::get_aws_points(z = zoom) %>% {.[[1]]} %>%
    st_drop_geometry() %>%
    select(site_id, elevation)

}


bind_to_csv <- function(fileout, ...){
  bind_rows(...) %>% write_csv(fileout)
}
