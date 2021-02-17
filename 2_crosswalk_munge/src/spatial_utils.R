buffer_sf <- function(out_ind, poly_sf_ind, site_ids, buffer){
  data_file <- as_data_file(out_ind)
  poly_sf <- scipiper::sc_retrieve(poly_sf_ind) %>% readRDS() %>% filter(site_id %in% site_ids)

  pb <- progress::progress_bar$new(
    format = "  buffering [:bar] :percent eta: :eta",
    total = nrow(poly_sf))
  buffered_sf <- st_buffer(poly_sf, dist = buffer)

  sf_donut_lakes <- buffered_sf %>% dplyr::select(site_id, Shape)
  for (j in 1:nrow(sf_donut_lakes)){
    sf_donut_lakes[j, ] <- suppressMessages(st_difference(buffered_sf[j, ], poly_sf[j, ]) %>% dplyr::select(site_id, Shape))
    pb$tick()
  }

  saveRDS(sf_donut_lakes, file = data_file)
  gd_put(out_ind, data_file)
}
