

NLDAS_datarod_tasks <- function(out_ind, NLDAS_cells, NLDAS_sf_grid, tmp_dir, ...){

  sources = c(...)

  data_file <- as_data_file(out_ind)
  file_bits <- stringr::str_split(basename(out_ind), pattern = '_')[[1]]

  remakefile <- basename(out_ind) %>% tools::file_path_sans_ext() %>% tools::file_path_sans_ext() %>% sprintf('%s_task_remakefile.yml', .)

  nldas_var <- file_bits[2]
  start_str <- file_bits[3] %>% stringr::str_split('-') %>% {.[[1]][1]} %>%
    as.Date(format = '%Y%m%d') %>% format('%Y-%m-%dT%H')
  stop_str <- file_bits[3] %>% stringr::str_split('-') %>% {.[[1]][2]} %>%
    as.Date(format = '%Y%m%d') %>% format('%Y-%m-%dT%H')

  url_pattern <- 'https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/access/timeseries.cgi?variable=NLDAS:NLDAS_FORA0125_H.002:%s&location=GEOM:POINT(%g,%%20%g)&startDate=%s&endDate=%s&type=asc2'


  sf_cells <- NLDAS_sf_grid %>% right_join(NLDAS_cells, by = c('x','y')) %>%
    # NLDAS_time[0.359420]_x[263]_y[119].csv
    mutate(task_name = file.path(tmp_dir, sprintf("NLDAS_%s_%s_x[%s]_y[%s].txt", nldas_var, file_bits[3], x, y)))

  tasks <- pull(sf_cells, task_name)

  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s', task_name)
    },
    command = function(task_name, ...){
      this_centroid <- sf_cells %>% filter(task_name == !!task_name) %>% st_centroid() %>% st_coordinates()
      this_url <- sprintf(url_pattern, nldas_var, this_centroid[1], this_centroid[2], start_str, stop_str)
      sprintf("download_nldas_datarods(outfile = target_name, url = I('%s'))", this_url)
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
    packages = c('httr'),
    sources = sources,
    final_targets = data_file,
    finalize_funs = 'bind_cols_datarods',
    tickquote_combinee_objects=TRUE,
    as_promises=TRUE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile, n_cores = 4)

  gd_put(out_ind, data_file)
  file.remove(remakefile)
}

bind_cols_datarods <- function(fileout, ...){

  data_files <- c(...)

  purrr::map(data_files, function(x){
    col_name <- stringr::str_split(basename(x), pattern = '_')[[1]][c(2,4,5)] %>% paste(collapse = '_') %>% tools::file_path_sans_ext()
    daily_data <- data.table::fread(x, skip = 40) %>% setNames(c('Date','time', 'data')) %>%
      mutate(datetime = as.POSIXct(paste0(Date,time), tz = 'UTC', format = "%Y-%m-%d%HZ") %>% lubridate::with_tz("Etc/GMT+6")) %>%
      select(datetime, data) %>%
      mutate(date = as.character(lubridate::floor_date(datetime, unit = 'days'))) %>% group_by(date) %>%
      summarize(data = mean(data), n = length(date), .groups = 'drop') %>%
      filter(n == 24) %>% select(date, data) %>% setNames(c('date',col_name))
    # gets overwritten continuously, but will be the same for all, or we'll fail on the bind_cols()
    dat_vec <<- daily_data$date
    return(daily_data %>% select(-date))
  }) %>% purrr::reduce(bind_cols) %>% mutate(date = dat_vec) %>% relocate(date)
  browser()
}

download_nldas_datarods <- function(outfile, url){
  download.file(url, destfile = outfile, method = 'curl', quiet = TRUE)
}
