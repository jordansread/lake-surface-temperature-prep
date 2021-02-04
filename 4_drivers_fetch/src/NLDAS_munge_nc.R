
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
