fetch_ned_raster <- function(out_ind, zoom, poly_sf_ind, centroid_sf_ind, box_res, dummy, ...){

  poly_sf <- scipiper::sc_retrieve(poly_sf_ind) %>% readRDS()

  # needed to determine which box these lakes are in
  centroids_sf <- scipiper::sc_retrieve(centroid_sf_ind) %>% readRDS() %>%
    filter(site_id %in% poly_sf$site_id)

  # need order to the be same
  stopifnot(all(centroids_sf$site_id == poly_sf$site_id))

  data_file <- as_data_file(out_ind)

  base_task_type <- basename(data_file) %>% tools::file_path_sans_ext()
  remakefile <- base_task_type %>% paste0('_tasks.yml')
  sources <- c(...)

  bbox_grid <- sf::st_make_grid(poly_sf, square = TRUE, cellsize = box_res, offset = c(-180,15))
  overlapping_boxes <- st_intersects(bbox_grid, poly_sf, sparse = F) %>% rowSums() %>% as.logical() %>% which()

   # write file of buffers that fit in each box
  # rows are centroids, columns are boxes. Contents are logicals:
  box_df <- st_within(centroids_sf, bbox_grid, sparse = F) %>%
    as_tibble(.name_repair = function(x){paste0('ned_bbox_', seq_len(length(x)))})
  # Define task table rows
  tasks <- paste0('ned_bbox_',overlapping_boxes)

  tmp_ned_dir <- 'tmp/ned_sf'
  dir.create(tmp_ned_dir)

  task_files <- tibble(task_name = tasks) %>%
    mutate(filename = sprintf('%s/%s_sf_buffers.rds', tmp_ned_dir, task_name))

  purrr::map(task_files$filename, function(x){
    taskname <- filter(task_files, filename == x) %>% pull(task_name)
    saveRDS(poly_sf[box_df[[taskname]], ], x)
  })
  # Define task table columns
  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s', task_name)
    },
    command = function(task_name, ...){
      filename <- task_files %>% filter(task_name == !!task_name) %>% pull(filename)
      sprintf("get_ned_buffers('%s', zoom = %s, dummy = I('%s'))", filename, zoom, dummy)
    }
  )

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(download_step),
    final_steps = c('download'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('elevatr','dplyr', 'readr'),
    sources = sources,
    final_targets = data_file,
    finalize_funs = 'bind_to_csv',
    tickquote_combinee_objects=TRUE,
    as_promises=TRUE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile, n_cores = 4)

  gd_put(out_ind, data_file)
  file.remove(remakefile)
  unlink(tmp_ned_dir, recursive = TRUE)

}


fetch_ned_points <- function(out_ind, zoom, centroid_sf_ind, box_res, dummy, ...){

  # needed to determine which box these lakes are in
  centroids_sf <- scipiper::sc_retrieve(centroid_sf_ind) %>% readRDS()

  data_file <- as_data_file(out_ind)

  base_task_type <- basename(data_file) %>% tools::file_path_sans_ext()
  remakefile <- base_task_type %>% paste0('_tasks.yml')
  sources <- c(...)

  bbox_grid <- sf::st_make_grid(centroids_sf, square = TRUE, cellsize = box_res, offset = c(-180,15))
  overlapping_boxes <- st_intersects(bbox_grid, centroids_sf, sparse = F) %>% rowSums() %>% as.logical() %>% which()

  # write file of buffers that fit in each box
  # rows are centroids, columns are boxes. Contents are logicals:
  box_df <- st_within(centroids_sf, bbox_grid, sparse = F) %>%
    as_tibble(.name_repair = function(x){paste0('ned_bbox_', seq_len(length(x)))})
  # Define task table rows
  tasks <- paste0('ned_bbox_',overlapping_boxes)

  tmp_ned_dir <- 'tmp/ned_pt_sf'
  dir.create(tmp_ned_dir)

  task_files <- tibble(task_name = tasks) %>%
    mutate(filename = sprintf('%s/%s_sf_points.rds', tmp_ned_dir, task_name))

  purrr::map(task_files$filename, function(x){
    taskname <- filter(task_files, filename == x) %>% pull(task_name)
    saveRDS(centroids_sf[box_df[[taskname]], ], x)
  })
  # Define task table columns
  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s', task_name)
    },
    command = function(task_name, ...){
      filename <- task_files %>% filter(task_name == !!task_name) %>% pull(filename)
      sprintf("get_ned_points('%s', zoom = %s, dummy = I('%s'))", filename, zoom, dummy)
    }
  )

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(download_step),
    final_steps = c('download'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('elevatr','dplyr', 'readr', 'sf'),
    sources = sources,
    final_targets = data_file,
    finalize_funs = 'bind_to_csv',
    tickquote_combinee_objects=TRUE,
    as_promises=TRUE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile, n_cores = 1)

  gd_put(out_ind, data_file)
  file.remove(remakefile)
  unlink(tmp_ned_dir, recursive = TRUE)

}


