

create_metadata_file <- function(outfile, wqp_site_cell_indices, lakes_sf_ind, wqp_data_fl){

  lakes_sf <- sc_retrieve(lakes_sf_ind) %>% readRDS()
  wqp_data <- feather::read_feather(wqp_data_fl)

  is_below <- function(filepath, size){
    file.size(filepath) < size
  }
  'TMP2m DLWRFsfc DSWRFsfc UGRD10m VGRD10m'
  dropped_sites <- wqp_site_cell_indices %>%
    mutate(TMP2m_datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_TMP2m_19790102-20210102_x[%s]_y[%s].txt", x, y),# TMP2m_fl_size = file.size(TMP2m_datarod_fl),
           DLWRFsfc_datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_DLWRFsfc_19790102-20210102_x[%s]_y[%s].txt", x, y),# DLWRFsfc_fl_size = file.size(DLWRFsfc_datarod_fl),
           DSWRFsfc_datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_DSWRFsfc_19790102-20210102_x[%s]_y[%s].txt", x, y), #DSWRFsfc_fl_size = file.size(DSWRFsfc_datarod_fl),
           UGRD10m_datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_UGRD10m_19790102-20210102_x[%s]_y[%s].txt", x, y), #UGRD10m_fl_size = file.size(UGRD10m_datarod_fl),
           VGRD10m_datarod_fl = sprintf("/Volumes/ThunderBlade/NLDAS_datarods/NLDAS_VGRD10m_19790102-20210102_x[%s]_y[%s].txt", x, y)) %>%  #VGRD10m_fl_size = file.size(VGRD10m_datarod_fl)) %>%
    filter_at(vars(-site_id, -x, -y), any_vars(is_below(., 600))) %>% pull(site_id)

  wqp_site_cell_indices <- wqp_site_cell_indices %>% filter(!site_id %in% dropped_sites, site_id %in% wqp_data$site_id)

  # add lat/lon
  lakes_sf %>% inner_join(wqp_site_cell_indices) %>%
    mutate(area_m2 = st_area(Shape)) %>%
    # can't do coords on polygons, so convert to centroids
    st_centroid() %>% mutate(lon = {st_coordinates(Shape)[,1]}, lat = {st_coordinates(Shape)[,2]}) %>%
    st_drop_geometry() %>%
    write_csv(outfile)

}
