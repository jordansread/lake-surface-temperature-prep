
crosswalk_points_in_poly <- function(ind_file, poly_ind_file, points_ind_file, points_ID_name){
  poly_data <- gd_get(ind_file = poly_ind_file) %>% readRDS

  points_data <- gd_get(ind_file = points_ind_file) %>% readRDS %>%
    rename(!!points_ID_name := "site_id")

  stopifnot('site_id' %in% names(poly_data))


  crosswalked_points <- st_join(points_data, poly_data, join = st_intersects) %>%
    filter(!is.na(site_id))

  crosswalked_ids <- st_drop_geometry(crosswalked_points) %>%
    dplyr::select(site_id, !!points_ID_name, everything())

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalked_ids, data_file)
  gd_put(ind_file, data_file)
}

centroid_sf_lakes <- function(out_ind, lake_ind, remove_ids){

  data_file <- scipiper::as_data_file(out_ind)

  sf_lakes <- scipiper::sc_retrieve(lake_ind) %>% readRDS() %>%
    filter(!site_id %in% remove_ids) %>% st_centroid() %>%
    saveRDS(data_file)

  gd_put(out_ind, data_file)

}
