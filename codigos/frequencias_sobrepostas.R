library(data.table)
library(dplyr)
library(gtfstools)
library(lubridate)

#gtfs <- read_gtfs("./dados/gtfs/2023/gtfs_rio-de-janeiro.zip")

gtfs <- read_gtfs("./dados/gtfs/2023/SPPO_2023_04_01Q_PROC.zip")


frequencias <- gtfs$frequencies %>%
  left_join(
    select(
      gtfs$trips,
      trip_id,
      direction_id,
      service_id,
      trip_short_name,
      trip_headsign
    )
  ) %>%
  arrange(start_time, direction_id, service_id) %>%
  group_by(trip_short_name, direction_id, service_id) %>%
  mutate(
    start_time = hms(start_time),
    end_time = hms(end_time),
    hora_st = lubridate::hour(start_time),
    hora_et = lubridate::hour(end_time)
  ) %>%
  mutate(
    start_time = if_else(
      hora_st > 23,
      start_time - lubridate::hours(24) + lubridate::days(1),
      start_time
    ),
    end_time = if_else(
      hora_et > 23,
      end_time - lubridate::hours(24) + lubridate::days(1),
      end_time
    )
  ) 

frequencias <- frequencias %>%
  mutate(
    start_time = as.POSIXct(start_time, origin = Sys.Date(),tz='GMT'),
    end_time = as.POSIXct(end_time, origin = Sys.Date(),tz='GMT')
  ) %>%
  arrange(start_time) %>%
  mutate(gap = as.numeric(difftime(start_time, lag(end_time), units = 'secs'))) %>% 
  mutate(fim_anterior = lag(end_time),
         inicio_anterior = lag(start_time),
         intervalo_anterior = lag(headway_secs))

sobreposicoes <- frequencias %>%
  filter(gap < 0) %>%
  select(trip_short_name,
         trip_headsign,
         service_id,
         start_time,
         end_time,
         headway_secs,
         inicio_anterior,
         fim_anterior,
         intervalo_anterior,
         direction_id) %>%
  arrange(trip_short_name, service_id, direction_id) %>% 
  ungroup() %>% 
  select(-c(direction_id)) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(start_time),Sys.Date(),units = "days"))) %>% 
  mutate(start_time = hms(as.ITime(start_time))+lubridate::hours(dif_days*24)) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(end_time),Sys.Date(),units = "days"))) %>% 
  mutate(end_time = lubridate::hms(as.ITime(end_time))+lubridate::hours(dif_days*24)) %>% 
  mutate(start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
                            sprintf("%02d", lubridate::minute(start_time)),
                            sprintf("%02d", lubridate::second(start_time)),sep=':'),
         end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
                          sprintf("%02d", lubridate::minute(end_time)),
                          sprintf("%02d", lubridate::second(end_time)),sep=':')) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(fim_anterior),Sys.Date(),units = "days"))) %>% 
  mutate(fim_anterior = hms(as.ITime(fim_anterior))+lubridate::hours(dif_days*24)) %>% 
  mutate(fim_anterior = paste(sprintf("%02d", lubridate::hour(fim_anterior)),
                            sprintf("%02d", lubridate::minute(fim_anterior)),
                            sprintf("%02d", lubridate::second(fim_anterior)),sep=':')) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(inicio_anterior),Sys.Date(),units = "days"))) %>% 
  mutate(inicio_anterior = hms(as.ITime(inicio_anterior))+lubridate::hours(dif_days*24)) %>% 
  mutate(inicio_anterior = paste(sprintf("%02d", lubridate::hour(inicio_anterior)),
                              sprintf("%02d", lubridate::minute(inicio_anterior)),
                              sprintf("%02d", lubridate::second(inicio_anterior)),sep=':')) %>% 
  select(-c(dif_days)) %>% 
  arrange(trip_short_name,trip_headsign,desc(service_id))

vacuos <- frequencias %>%
  filter(gap > 0) %>%
  select(trip_short_name,
         trip_headsign,
         service_id,
         start_time,
         end_time,
         headway_secs,
         fim_anterior,
         direction_id,
         gap) %>%
  arrange(trip_short_name, service_id, direction_id) %>% 
  ungroup() %>% 
  select(-c(direction_id)) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(start_time),Sys.Date(),units = "days"))) %>% 
  mutate(start_time = hms(as.ITime(start_time))+lubridate::hours(dif_days*24)) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(end_time),Sys.Date(),units = "days"))) %>% 
  mutate(end_time = lubridate::hms(as.ITime(end_time))+lubridate::hours(dif_days*24)) %>% 
  mutate(start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
                            sprintf("%02d", lubridate::minute(start_time)),
                            sprintf("%02d", lubridate::second(start_time)),sep=':'),
         end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
                          sprintf("%02d", lubridate::minute(end_time)),
                          sprintf("%02d", lubridate::second(end_time)),sep=':')) %>% 
  mutate(dif_days = as.integer(difftime(as.Date(fim_anterior),Sys.Date(),units = "days"))) %>% 
  mutate(fim_anterior = hms(as.ITime(fim_anterior))+lubridate::hours(dif_days*24)) %>% 
  mutate(fim_anterior = paste(sprintf("%02d", lubridate::hour(fim_anterior)),
                              sprintf("%02d", lubridate::minute(fim_anterior)),
                              sprintf("%02d", lubridate::second(fim_anterior)),sep=':')) %>% 
  select(-c(dif_days)) %>% 
  arrange(trip_short_name,trip_headsign,desc(service_id))

fwrite(sobreposicoes,"./sobreposicoes.csv",sep = ';', dec=',')
fwrite(vacuos,"./vacuos.csv",sep = ';', dec=',')

