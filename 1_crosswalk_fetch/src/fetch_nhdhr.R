


create_nhd_HR_download_plan <- function(states, min_size, d_tolerance, remove_IDs = NULL, keep_IDs = NULL){
  base_url <- 'ftp://rockyftp.cr.usgs.gov/vdelivery/Datasets/Staged/Hydrography/NHD/State/HighResolution/GDB/NHD_H_%s_State_GDB.zip'

  fetch_as_sf_step <- create_task_step(
    step_name = 'fetch_NHD_as_sf',
    target_name = function(task_name, step_name, ...) {
      sprintf('NHDHR_%s_sf', task_name)
    },
    command = function(task_name, step_name, ...) {
      state_url <- sprintf(base_url, task_name)
      sprintf("fetch_NHD_as_sf(url = I('%s'), min_size = %s, d_tol = %s)", state_url, min_size, d_tolerance) # add remove/keep_IDs here later...
    }
  )


  create_task_plan(states, list(fetch_as_sf_step), final_steps = 'fetch_NHD_as_sf', add_complete = FALSE)
}

create_nhd_HR_download_makefile <- function(makefile, task_plan, final_targets){
  include <- "1_crosswalk_fetch.yml"
  packages <- c('dplyr','sf', 'lwgeom')
  sources <- '1_crosswalk_fetch/src/fetch_nhdhr.R'

  create_task_makefile(task_plan, makefile, include = include, packages = packages,
                       sources = sources, finalize_funs = c('combine_nhd_sfs','GNIS_Name_xwalk'),
                       final_targets = final_targets)
}

#' @param min_size minimum size in m^2 for lakes to keep.
#' 4 ha is 40000 m^2
fetch_NHD_as_sf <- function(url,min_size, d_tol){
  dl_dest <- tempfile(pattern = "NHD_", tmpdir = tempdir(), fileext = "tmp_nhd.zip")
  unzip_dir <- tempfile(pattern = "NHD_unzip_", tmpdir = tempdir(), fileext = "")
  dir.create(unzip_dir)

  # could class(min_size) <- 'units' then add numerator <- c("m","m") and denominator <- c() to attributes?
  on.exit({
    unlink(unzip_dir, recursive = TRUE)
    unlink(dl_dest)
  })

  download.file(url, destfile = dl_dest, quiet = TRUE)
  unzip(dl_dest, exdir = unzip_dir)

  waterbodies_sf <- sf::read_sf(file.path(unzip_dir,paste0(tools::file_path_sans_ext(basename(url)), '.gdb')), layer = 'NHDWaterbody') %>%
    filter(FType %in% c(390, 436, 361))  #select only lakes/ponds/reservoirs. This drops things like swamp/marsh

  waterbodies_sf <- sf::st_transform(waterbodies_sf, crs = "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs")
  waterbodies_sf$area_m2 <- as.numeric(sf::st_area(waterbodies_sf))# in m2, stripping "units" class because I don't know how to filter against

  waterbodies_sf %>%
    filter(area_m2 > min_size) %>%
    mutate(site_id = paste0('nhdhr_', Permanent_Identifier)) %>% dplyr::select(site_id, GNIS_Name) %>%  #geometry selected automatically
    sf::st_simplify(preserveTopology = TRUE, dTolerance = d_tol) %>%
    sf::st_transform(crs = 4326)
}

GNIS_Name_xwalk <- function(ind_file, ...){
  data_file <- scipiper::as_data_file(ind_file)

  sf_lakes <- rbind(...)
  deduped_sf_lakes <- st_drop_geometry(sf_lakes[!duplicated(sf_lakes$site_id), ]) %>%
    dplyr::select(site_id, GNIS_Name)

  saveRDS(deduped_sf_lakes, data_file)
  gd_put(ind_file, data_file)
}

combine_nhd_sfs <- function(ind_file, ...){
  data_file <- scipiper::as_data_file(ind_file)

  sf_lakes <- rbind(...)
  deduped_sf_lakes <- sf_lakes[!duplicated(sf_lakes$site_id), ] %>%
    dplyr::select(site_id)

  saveRDS(deduped_sf_lakes, data_file)
  gd_put(ind_file, data_file)
}
