# munge wqp data
munge_wqp_temperature <- function(outind, wqp_ind, wqp_crosswalk_ind){

  outfile <- as_data_file(outind)

  wqp2nhd <- sc_retrieve(wqp_crosswalk_ind) %>% readRDS() %>%
    distinct()


  wqp_temp_data <- scipiper::sc_retrieve(wqp_ind) %>% readRDS(wqp_ind) #
  # from original lake temp repo: https://github.com/USGS-R/necsc-lake-modeling/blob/master/scripts/download_munge_wqp.R
  max_temp <- 40 # threshold!
  min_temp <- 0
  max_depth <- 260
  zero_doy <- c(190, 290) # remove all zeros within this range


  depth_unit_map <- data.frame(depth.units=c('meters','m','in','ft','feet','cm', 'mm', NA),
                               depth.convert = c(1,1,0.0254,0.3048,0.3048,0.01, 0.001, NA),
                               stringsAsFactors = FALSE)

  var_unit_map <- data.frame(units=c("deg C","deg F", NA),
                             convert = c(1, 1/1.8,NA),
                             offset = c(0,-32,NA),
                             stringsAsFactors = FALSE)

  activity.sites <- group_by(wqp_temp_data, OrganizationIdentifier) %>%
    summarize(act.n = sum(!is.na(`ActivityDepthHeightMeasure/MeasureValue`)), res.n=sum(!is.na((`ResultDepthHeightMeasure/MeasureValue`)))) %>%
    mutate(use.depth.code = ifelse(act.n>res.n, 'act','res')) %>%
    dplyr::select(OrganizationIdentifier, use.depth.code)

  left_join(wqp_temp_data, activity.sites, by='OrganizationIdentifier') %>%
    mutate(raw.depth = case_when(
      use.depth.code == 'act' ~ `ActivityDepthHeightMeasure/MeasureValue`,
      use.depth.code == 'res' ~ as.numeric(`ResultDepthHeightMeasure/MeasureValue`) #as of 10/25/2019, the chars that will fail conversion are things like "Haugen Lake Littoral", "Burns Lake Littoral", "Littoral Zone Sample"
    ),
    depth.units = case_when(
      use.depth.code == 'act' ~ `ActivityDepthHeightMeasure/MeasureUnitCode`,
      use.depth.code == 'res' ~ `ResultDepthHeightMeasure/MeasureUnitCode`
    )) %>%
    rename(Date = ActivityStartDate,
           raw_value = ResultMeasureValue,
           units = `ResultMeasure/MeasureUnitCode`,
           result_method = `ResultAnalyticalMethod/MethodIdentifier`,
           timezone = `ActivityStartTime/TimeZoneCode`) %>%
    mutate(time = substr(`ActivityStartTime/Time`, 0, 5)) %>%
    dplyr::select(Date, time, timezone, raw_value, units, raw.depth, depth.units, MonitoringLocationIdentifier, result_method) %>%
    left_join(var_unit_map, by='units') %>%
    left_join(depth_unit_map, by='depth.units') %>%
    mutate(wtemp = convert * (raw_value + offset), depth = raw.depth * depth.convert, doy = lubridate::yday(Date)) %>%
    filter(!is.na(wtemp), !is.na(depth), wtemp <= max_temp, wtemp >= min_temp, depth <= max_depth, !result_method %in% c('LAB TEMP','LAB'),
           !(doy %in% zero_doy[1]:zero_doy[2] & wtemp ==0)) %>%
    dplyr::select(Date, time, timezone, MonitoringLocationIdentifier, depth, wtemp) %>%
    inner_join(dplyr::select(wqp2nhd, site_id, MonitoringLocationIdentifier)) %>%
    filter(depth <= 1) %>%
    group_by(Date, site_id) %>% summarise(wtemp = mean(wtemp, na.rm = TRUE)) %>%
    filter(Date > as.Date('1979-01-01')) %>%
    feather::write_feather(outfile)
  gd_put(outind, outfile)
}


combine_temp_sources <- function(outind, wqp_daily_ind, superset_daily_ind, cell_sites, remove_ids){


  outfile <- as_data_file(outind)
  wqp_daily <- sc_retrieve(wqp_daily_ind) %>% read_feather()
  superset_daily <- sc_retrieve(superset_daily_ind) %>% read_feather() %>% select(site_id, Date = date, depth, temp) %>%
    filter(depth <= 1) %>% group_by(Date, site_id) %>% summarise(wtemp = mean(temp, na.rm = TRUE), .groups = 'drop')

  new_data <- anti_join(superset_daily, wqp_daily, by = c('site_id', 'Date'))
  bind_rows(wqp_daily, new_data) %>% arrange(site_id) %>%
    filter(!site_id %in% remove_ids) %>%
    filter(site_id %in% cell_sites$site_id) %>%
    write_feather(path = outfile)


  gd_put(outind, outfile)
}
