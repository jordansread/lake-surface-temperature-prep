

create_metadata_file <- function(outfile, site_cell_indices, lakes_sf_ind, obs_ind_fl, elevation_ind_fl){
  
  lakes_sf <- sc_retrieve(lakes_sf_ind) %>% readRDS()
  
  obs_sites <- sc_retrieve(obs_ind_fl) %>% feather::read_feather() %>% pull(site_id) %>% unique()
  
  
  elevations <- sc_retrieve(elevation_ind_fl) %>% read_csv()
  
  # add lat/lon
  lakes_sf %>% inner_join(site_cell_indices) %>%
    # observed: TRUE/FALSE
    mutate(observed = site_id %in% obs_sites, area_m2 = st_area(Shape)) %>%
    # can't do coords on polygons, so convert to centroids
    st_centroid() %>% mutate(lon = {st_coordinates(Shape)[,1]}, lat = {st_coordinates(Shape)[,2]}) %>%
    st_drop_geometry() %>%
    select(-Elevation, -FType, -FCode) %>%
    inner_join(elevations, by = 'site_id') %>%
    write_csv(outfile)
  
}
