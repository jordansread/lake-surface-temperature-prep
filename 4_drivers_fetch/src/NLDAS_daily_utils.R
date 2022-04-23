
get_datetime <- function(indx){
  as.POSIXct('1979-01-01 13:00', tz = 'GMT') %>% seq(from = ., by = 'hours', length.out = indx+1) %>%
    tail(1)
}


filter_nldas_hash  <- function(hash_fl, var_name, out_dir){

  yaml::yaml.load_file(hash_fl) %>% names() %>%
    tibble(filepath = .) %>%
    mutate(var = stringr::str_extract(filepath, '(?<=var\\[).+?(?=\\])'),
           t_start = stringr::str_extract(filepath, '(?<=time\\[).+?(?=\\.)') %>% as.numeric(),
           # using {0,30} (min to max matching length to avoid infinite span)
           t_stop = stringr::str_extract(filepath, '(?<=time\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric(),
           x_start = stringr::str_extract(filepath, '(?<=x\\[).+?(?=\\.)') %>% as.numeric(),
           x_stop = stringr::str_extract(filepath, '(?<=x\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric(),
           y_start = stringr::str_extract(filepath, '(?<=y\\[).+?(?=\\.)') %>% as.numeric(),
           y_stop = stringr::str_extract(filepath, '(?<=y\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric()) %>%
    filter(var == var_name)
}


reduce_hour2daily_nldas <- function(nc_file, hash_tbl, chunk_size){
  nc_file_info <- hash_tbl

  # just using this for progress bar:
  t_start <- stringr::str_extract(nc_file_info$filepath, '(?<=time\\[).+?(?=\\.)') %>% as.numeric() %>% min()
  t_stop <- stringr::str_extract(nc_file_info$filepath, '(?<=time\\[[0-9]\\w{0,30}\\.).+?(?=\\])') %>% as.numeric() %>% max()
  t_count <- seq(t_start, to = t_stop, by = chunk_size)

  pb <- progress::progress_bar$new(total = length(t_count)*length(unique(nc_file_info$var)), format = "  processing to netcdf [:bar] :percent in :elapsed")

  y_start <- pull(nc_file_info, y_start) %>% unique()
  x_start <- pull(nc_file_info, x_start) %>% unique()
  t_start <- pull(nc_file_info, t_start) %>% min() %>% get_datetime()
  y_stop <- pull(nc_file_info, y_stop) %>% unique()
  x_stop <- pull(nc_file_info, x_stop) %>% unique()
  t_stop <- pull(nc_file_info, t_stop) %>% max() %>% get_datetime()
  var <- pull(nc_file_info, var) %>% unique()

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
  
  cubed_daily <- function(x){
  	sum(x^3 / 24)
  }

  daily_funs <- c(apcpsfc = mean_daily,
  								dlwrfsfc = mean_daily,
  								dswrfsfc = mean_daily,
  								pressfc = mean_daily,
  								spfh2m = mean_daily,
  								tmp2m = mean_daily,
  								ugrd10m = cubed_daily,
  								vgrd10m = cubed_daily)
  if (file.exists(nc_file)){
    unlink(nc_file)
  }

  output_nc_file <- nc_create(nc_file, var_values, force_v4 = TRUE)
  ncvar_put(output_nc_file, varid = var, vals = rep(0, (length(x_indx))*(length(y_indx))*(length(dates)+1)),
            start= c(1, 1, 1), count = c(-1, -1, -1), verbose=FALSE)

  nc_close(output_nc_file)
  for (nc_sub_file in unique(nc_file_info$filepath)){
    output_nc_file <- nc_open(nc_file, write=TRUE)
    this_chunk <- filter(nc_file_info, filepath == nc_sub_file)

    time_chunk_lead <- seq(this_chunk[["t_start"]], this_chunk[["t_stop"]], by = chunk_size) - this_chunk[["t_start"]] + 1# MINUS t_start?
    time_chunk_follow <- c(tail(time_chunk_lead, -1L) - 1, this_chunk[["t_stop"]] - this_chunk[["t_start"]] + 1)


    hourly_nc <- nc_open(nc_sub_file, suppress_dimvals = TRUE)

    for (chunk_idx in 1:length(time_chunk_lead)){
      time_lead <- time_chunk_lead[chunk_idx]
      time_follow <- time_chunk_follow[chunk_idx]
      time_count <- time_follow - time_lead + 1

      # the "time_lead" and follow restart per new nc_sub_file, because they are file indexes.
      # The index for the new file and dates needs to keep growing when we hit a new file:
      day_lead <- time_lead + this_chunk[["t_start"]]
      day_follow <- time_follow + this_chunk[["t_start"]]

      tmp_data <- ncvar_get(hourly_nc, varid = var, start = c(1, 1, time_lead), count = c(-1, -1, time_count)) %>%
        slam::rollup(3L, INDEX = day_indx[day_lead:day_follow], daily_funs[[var]])


      # start and finish of time
      nc_day_indx <- day_indx[day_lead:day_follow] %>% range() %>% {.+1}

      # need to pull the existing data (anything empty would be zero) so we can add existing, since we don't always have complete days
      overwrite_data <- ncvar_get(output_nc_file, varid = var, start = c(1, 1, nc_day_indx[1]), count = c(-1, -1, nc_day_indx[2] - nc_day_indx[1] + 1))
      ncvar_put(output_nc_file, varid = var, vals = overwrite_data + tmp_data, start= c(1, 1, nc_day_indx[1]), count = c(-1, -1, nc_day_indx[2] - nc_day_indx[1] + 1), verbose=FALSE)
      pb$tick()
      rm(tmp_data)
      gc()
    }
    
    nc_close(hourly_nc)
    # close each time so we can interrogate this as it grows
    nc_close(output_nc_file)
  }
  # variables that use cubed daily need a cubed root applied:
  if (var %in% c('ugrd10m', 'vgrd10m')){
  	output_nc_file <- nc_open(nc_file, write=TRUE)
  	raw_data <- ncvar_get(output_nc_file, varid = var, start = c(1, 1, 1), count = c(-1, -1, -1))
  	message('taking the cube root of ', var)
  	is_neg_real <- raw_data < 0 & !is.na(raw_data)
  	# make negative numbers positive:
  	raw_data[is_neg_real] <- raw_data[is_neg_real] * (-1)
  	# apply cubed root to each individual daily value:
  	cubed_data <- raw_data ^ (1/3)
  	# convert back to negative numbers
  	cubed_data[is_neg_real] <- cubed_data[is_neg_real] * (-1)
  	rm(raw_data)
  	rm(is_neg_real)
  	ncvar_put(output_nc_file, varid = var, vals = cubed_data, start = c(1,1,1), count = c(-1,-1,-1), verbose=FALSE)
  	nc_close(output_nc_file)
  }
}
