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
    # "Temp at lab-iced." "Temp. at lab" "Temperature at lab" clearly need to be removed 
    # !grepl(pattern="(Temp at lab|Temp. at lab|Temperature at lab)", ResultCommentText
    rename(Date = ActivityStartDate,
           raw_value = ResultMeasureValue,
           units = `ResultMeasure/MeasureUnitCode`,
           result_method = `ResultAnalyticalMethod/MethodIdentifier`,
           timezone = `ActivityStartTime/TimeZoneCode`) %>%
    mutate(time = substr(`ActivityStartTime/Time`, 0, 5)) %>%
    # remove bad data from labs:
    filter(!grepl(pattern="(Temp at lab|Temp. at lab|Temperature at lab)", ResultCommentText)) %>% 
    dplyr::select(Date, time, timezone, raw_value, units, raw.depth, depth.units, MonitoringLocationIdentifier, result_method, CharacteristicName) %>%
    left_join(var_unit_map, by='units') %>%
    left_join(depth_unit_map, by='depth.units') %>%
    mutate(wtemp = convert * (raw_value + offset), depth = raw.depth * depth.convert, doy = lubridate::yday(Date)) %>%
    filter(!is.na(wtemp), !is.na(depth), wtemp <= max_temp, wtemp >= min_temp, depth <= max_depth, !result_method %in% c('LAB TEMP','LAB'),
           !(doy %in% zero_doy[1]:zero_doy[2] & wtemp ==0)) %>%
    filter(!custom_wqx_qaqc(.)) %>% 
    dplyr::select(Date, time, timezone, source = MonitoringLocationIdentifier, depth, wtemp) %>%
    inner_join(dplyr::select(wqp2nhd, site_id, source = MonitoringLocationIdentifier)) %>%
    filter(depth <= 1, !is.na(wtemp)) %>%
    group_by(Date, site_id) %>% 
    # sort by depth (shallowest first) then temperature so we can take the warmest where there is an incorrectly entered profile
    arrange(depth, desc(wtemp)) %>% 
    summarise(wtemp = first(wtemp), source = first(source),
              .groups = 'drop') %>% 
    filter(Date > as.Date('1979-01-01')) %>%
    feather::write_feather(outfile)
  gd_put(outind, outfile)
}

#' there is a mix of bad and good data from IL EPA. The bad data seem to be of lower precision, 
#' which is what we're filtering out here
# as of June 2021, sites with "IL_EPA_WQX-" and "IL_EPA-" have a mix of lab and field data with no real way to tell. 
# the field data seems to be mostly low temperature ints (0,1,2,3,4,5,6), but there are some other bad values too that aren't ints
#' as of June 22, email from Jonathan Burian EPA Region 5 says that 'Temperature, sample' is reliably something we can filter on
custom_wqx_qaqc <- function(df){
  mutate(df, bad_flag = case_when(
    grepl('IL_EPA', MonitoringLocationIdentifier) & CharacteristicName == 'Temperature, sample' ~ TRUE,
    # this USGS location has bad data that has this one specific temperature and is always at 0 depth: 
    MonitoringLocationIdentifier == 'USGS-12301919' & raw.depth == 0 & raw_value == 21.3 ~ TRUE,
    # streak of bad repeating data at 21.3°
    MonitoringLocationIdentifier == 'USGS-12301830' & raw_value == 17.1 ~ TRUE,
    # way too cold for June for this location
    MonitoringLocationIdentifier == 'USGS-402803121303200' & raw_value == 0.5 & Date == as.Date('1981-06-16') ~ TRUE,
    # 35°C temperatures in Feb in WI? other mix of bad and good values for this site
    MonitoringLocationIdentifier == 'WIDNR_WQX-643126' ~ TRUE,
    # unreasonably bad data from these sites in Nevada. Mix of real and bad data on the same day: 
    MonitoringLocationIdentifier == 'MNPCA-11-0167-00-100' & raw_value == 5.6 & Date == as.Date("1995-08-30") ~ TRUE,
    grepl('21NEV1_WQX-NV04-108-R-', MonitoringLocationIdentifier) ~ TRUE, 
    grepl('21NEV1_WQX-NV03-104-R-', MonitoringLocationIdentifier) ~ TRUE, 
    grepl('21NEV1_WQX-NV08-203-R-', MonitoringLocationIdentifier) ~ TRUE, 
    grepl('21NEV1_WQX-NV04-106-R-', MonitoringLocationIdentifier) ~ TRUE, 
    grepl('21NEV1_WQX-NV09-304-L-', MonitoringLocationIdentifier) ~ TRUE, 
    TRUE ~ FALSE
  )) %>% pull(bad_flag)
}

combine_temp_sources <- function(outind, wqp_daily_ind, superset_daily_ind, cell_sites, remove_ids){

  # see https://github.com/USGS-R/lake-temperature-model-prep/issues/171
  remove_sources <- c('South_Center_DO_2018_09_11_All.rds', 'Carlos_DO_2018_11_05_All.rds', 'Greenwood_DO_2018_09_14_All.rds')
  outfile <- as_data_file(outind)
  wqp_daily <- sc_retrieve(wqp_daily_ind) %>% read_feather()
  
  superset_daily <- sc_retrieve(superset_daily_ind) %>% read_feather() %>% 
    # remove WQP sources since we want the fresh updates for those and these IDs won't join correctly
    filter(!grepl('wqp_', source)) %>% 
    mutate(source = basename(source)) %>% select(site_id, Date = date, depth, temp, source) %>%
    filter(depth <= 1, !source %in% remove_sources) %>% group_by(Date, site_id) %>% 
    summarise(wtemp = first(temp), source = first(source), 
              .groups = 'drop')
  new_data <- anti_join(superset_daily, wqp_daily, by = c('site_id', 'Date'))
  
  bind_rows(wqp_daily, new_data) %>% arrange(site_id) %>%
    filter(!site_id %in% remove_ids) %>%
    filter(site_id %in% cell_sites$site_id) %>%
    filter(Date < as.Date('2021-01-01'), Date > as.Date('1979-12-31')) %>% 
    group_by(Date, site_id) %>% 
    summarise(wtemp = first(wtemp), source = first(source), 
              .groups = 'drop') %>% 
    write_feather(path = outfile)


  gd_put(outind, outfile)
}
