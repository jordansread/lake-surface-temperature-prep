

build_daily_NLDAS_nc <- function(fileout, hash_fl, chunk_size, out_dir){

  get_datetime <- function(indx){
    as.POSIXct('1979-01-01 13:00', tz = 'GMT') %>% seq(from = ., by = 'hours', length.out = indx+1) %>%
      tail(1)
  }

  get_yeardate <- function(indx){
    get_datetime(indx) %>%
      lubridate::floor_date() %>% format('%Y%m%d')
  }

  nc_file_info <- yaml::yaml.load_file(hash_fl) %>% names() %>%
    tibble(filepath = .) %>%
    mutate(var = stringr::str_extract(filepath, '(?<=var\\[).+?(?=\\])'),
           t_start = stringr::str_extract(filepath, '(?<=time\\[).+?(?=\\.)') %>% as.numeric(),
           # using {0,30} (min to max matching length to avoid infinite span)
           t_stop = stringr::str_extract(filepath, '(?<=time\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric(),
           x_start = stringr::str_extract(filepath, '(?<=x\\[).+?(?=\\.)') %>% as.numeric(),
           x_stop = stringr::str_extract(filepath, '(?<=x\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric(),
           y_start = stringr::str_extract(filepath, '(?<=y\\[).+?(?=\\.)') %>% as.numeric(),
           y_stop = stringr::str_extract(filepath, '(?<=y\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric()) %>%
    filter(var == 'tmp2m') %>%
    group_by(var) %>%
    mutate(fileout = file.path(out_dir, sprintf('NLDAS_step[daily]_var[%s]_date[%s.%s].nc', var, get_yeardate(min(t_start)), get_yeardate(max(t_stop)))))

  # just using this for progress bar:
  t_start <- stringr::str_extract(nc_file_info$filepath, '(?<=time\\[).+?(?=\\.)') %>% as.numeric() %>% min()
  t_stop <- stringr::str_extract(nc_file_info$filepath, '(?<=time\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric() %>% max()
  t_count <- seq(t_start, to = t_stop, by = chunk_size)

  unq_nc_files <- unique(nc_file_info$fileout)


  pb <- progress::progress_bar$new(total = length(t_count)*length(unique(nc_file_info$var)), format = "  processing to netcdf [:bar] :percent in :elapsed")
  for (nc_file in unq_nc_files){

    these_details <- nc_file_info %>% filter(fileout == nc_file)
    y_start <- pull(these_details, y_start) %>% unique()
    x_start <- pull(these_details, x_start) %>% unique()
    t_start <- pull(these_details, t_start) %>% min() %>% get_datetime()
    y_stop <- pull(these_details, y_stop) %>% unique()
    x_stop <- pull(these_details, x_stop) %>% unique()
    t_stop <- pull(these_details, t_stop) %>% max() %>% get_datetime()
    var <- pull(these_details, var) %>% unique()

    # will fail if either x_start or x_stop is more than one number, which we don't want to support here
    x_indx <- seq(x_start, to = x_stop, by = 1)
    y_indx <- seq(y_start, to = y_stop, by = 1)


    # create nc file, initialize it

    stop_str <- stringr::str_extract(nc_file, '(?<=date\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>%
      as.Date(format = '%Y%m%d')
    start_str <- stringr::str_extract(nc_file, '(?<=date\\[).+?(?=\\.)') %>%
      as.Date(format = '%Y%m%d')


    dates <- seq(start_str, to = stop_str, by = 'days')
    datetimes <- seq(t_start, to = t_stop, by = 'hours')
    # starts at 7am central on day one, assuming t_start = 0, so 17 hours in the first day if 1979-01-01 13:00:00 GMT
    n_datetimes <- length(datetimes)
    day_indx <- c(rep(0, 17), floor(seq(1, by = 1/24, length.out = n_datetimes-17)))


    dim_x <- ncdim_def( "x", "index x", x_indx) # actual dims from NLDAS, will not change
    dim_y <- ncdim_def( "y", "index y", y_indx) # get actual dims
    dim_t <- ncdim_def( "Time", sprintf("days since %s", start_str), 0:length(dates), unlim=TRUE)

    var_values <- ncvar_def(var, "unknown",  list(dim_x, dim_y, dim_t), -999)

    mean_daily <- function(x){
      sum(x / 24)
    }

    if (file.exists(nc_file)){
      unlink(nc_file)
    }

    output_nc_file <- nc_create(nc_file, var_values, force_v4 = TRUE)
    ncvar_put(output_nc_file, varid = var, vals = rep(0, (length(x_indx))*(length(y_indx))*(length(dates)+1)),
              start= c(1, 1, 1), count = c(-1, -1, -1), verbose=FALSE)

    nc_close(output_nc_file)
    for (nc_sub_file in unique(these_details$filepath)){
      output_nc_file <- nc_open(nc_file, write=TRUE)
      this_chunk <- filter(these_details, filepath == nc_sub_file)

      time_chunk_lead <- seq(this_chunk[["t_start"]], this_chunk[["t_stop"]], by = chunk_size) - this_chunk[["t_start"]] + 1# MINUS t_start?
      time_chunk_follow <- c(tail(time_chunk_lead, -1L) - 1, this_chunk[["t_stop"]] - this_chunk[["t_start"]] + 1)


      hourly_nc <- nc_open(nc_sub_file, suppress_dimvals = TRUE)
      on.exit({nc_close(output_nc_file)
        nc_close(hourly_nc)})

      for (chunk_idx in 1:length(time_chunk_lead)){
        time_lead <- time_chunk_lead[chunk_idx]
        time_follow <- time_chunk_follow[chunk_idx]
        time_count <- time_follow - time_lead + 1

        # the "time_lead" and follow restart per new nc_sub_file, because they are file indexes.
        # The index for the new file and dates needs to keep growing when we hit a new file:
        day_lead <- time_lead + this_chunk[["t_start"]]
        day_follow <- time_follow + this_chunk[["t_start"]]
        tmp_data <- ncvar_get(hourly_nc, varid = var, start = c(1, 1, time_lead), count = c(-1, -1, time_count)) %>%
          slam::rollup(3L, INDEX = day_indx[day_lead:day_follow], mean_daily)


        # start and finish of time
        nc_day_indx <- day_indx[day_lead:day_follow] %>% range() %>% {.+1}

        # need to pull the existing data (anything empty would be zero) so we can add existing, since we don't always have complete days
        overwrite_data <- ncvar_get(output_nc_file, varid = var, start = c(1, 1, nc_day_indx[1]), count = c(-1, -1, nc_day_indx[2] - nc_day_indx[1] + 1))
        ncvar_put(output_nc_file, varid = var, vals = overwrite_data + tmp_data, start= c(1, 1, nc_day_indx[1]), count = c(-1, -1, nc_day_indx[2] - nc_day_indx[1] + 1), verbose=FALSE)
        pb$tick()
        gc()
      }
      nc_close(hourly_nc)
      # close each time so we can interrogate this as it grows
      nc_close(output_nc_file)
    }
  }


  sc_indicate(fileout, data_file = unq_nc_files)
}


get_yeardate <- function(indx){
  get_datetime(indx) %>%
    lubridate::floor_date() %>% format('%Y%m%d')
}

get_datetime <- function(indx){
  as.POSIXct('1979-01-01 13:00', tz = 'GMT') %>% seq(from = ., by = 'hours', length.out = indx+1) %>%
    tail(1)
}

NLDAS_hour2daily_tasks <- function(fileout, hash_fl, chunk_size, out_dir, ...){

  sources = c(...)

  remakefile <- basename(fileout) %>% tools::file_path_sans_ext() %>% tools::file_path_sans_ext() %>% sprintf('%s_task_remakefile.yml', .)

  task_tbl <- yaml::yaml.load_file(hash_fl) %>% names() %>%
    tibble(filepath = .) %>%
    mutate(var = stringr::str_extract(filepath, '(?<=var\\[).+?(?=\\])'),
           t_start = stringr::str_extract(filepath, '(?<=time\\[).+?(?=\\.)') %>% as.numeric(),
           # using {0,30} (min to max matching length to avoid infinite span)
           t_stop = stringr::str_extract(filepath, '(?<=time\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric()) %>%
    group_by(var) %>%
    mutate(fileout = file.path(out_dir, sprintf('NLDAS_step[daily]_var[%s]_date[%s.%s].nc', var, get_yeardate(min(t_start)), get_yeardate(max(t_stop)))))


  tasks <- pull(task_tbl, var) %>% unique()

  filter_step <- create_task_step(
    step_name = 'filter',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s_hashes', task_name)
    },
    command = function(task_name, ...){
      sprintf("filter_nldas_hash(hash_fl = '%s', var_name = I('%s'), out_dir = I('%s'))", hash_fl, task_name, out_dir)
    }
  )

  reduce_step <- create_task_step(
    step_name = 'reduce',
    target_name = function(task_name, step_name, ...) {
      filter(task_tbl, var == task_name) %>% pull(fileout) %>% unique()
    },
    command = function(task_name, ...){
      sprintf("reduce_hour2daily_nldas(nc_file = target_name, %s_hashes, chunk_size = %s)", task_name, chunk_size)
    }
  )

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(filter_step, reduce_step),
    final_steps = c('reduce'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('slam'),
    sources = sources,
    final_targets = fileout,
    finalize_funs = 'combine_to_ind',
    tickquote_combinee_objects=TRUE,
    as_promises=TRUE)

  scipiper::scmake(remake_file = remakefile)
  file.remove(remakefile)
}
