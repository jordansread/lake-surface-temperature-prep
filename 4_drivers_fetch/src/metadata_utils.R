

create_metadata_file <- function(outfile, wqp_site_cell_indices, lakes_sf_ind){

  lakes_sf <- sc_retrieve(lakes_sf_ind) %>% readRDS(lakes_sf_ind)

  dropped_sites <- wqp_site_cell_indices %>%
    mutate(datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_DLWRFsfc_19790102-20210102_x[%s]_y[%s].txt", x, y), fl_size = file.size(datarod_fl)) %>%
    filter(fl_size < 600) %>% pull(site_id)

  wqp_site_cell_indices <- wqp_site_cell_indices %>% filter(!site_id %in% dropped_sites)

  # add lat/lon
  lakes_sf %>% inner_join(wqp_site_cell_indices) %>%
    mutate(area_m2 = st_area(Shape)) %>%
    # can't do coords on polygons, so convert to centroids
    st_centroid() %>% mutate(lon = {st_coordinates(Shape)[,1]}, lat = {st_coordinates(Shape)[,2]}) %>%
    st_drop_geometry() %>%
    write_csv(outfile)

}
