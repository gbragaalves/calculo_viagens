library(data.table)
library(dplyr)
library(gtfstools)
library(lubridate)

ano <- '2023'
mes_gtfs <- '05'
quinzena_gtfs <- '01'
tipo_dia <- 'du'

end_qh <- paste0("./resultados/quadro_horario/",ano,"/",mes_gtfs
                 ,"/qh_",ano,"-",mes_gtfs,"-",quinzena_gtfs,"Q_",tipo_dia,
                 ".csv")

quadro <- fread(end_qh) %>% 
  select(servico,sentido,intervalo,start_time,end_time) %>% 
  mutate(intervalo = as.numeric(sub(",", ".", intervalo, fixed = TRUE))*60) %>% 
  rename(headway_secs=intervalo,
         trip_short_name = servico) %>% 
  mutate(direction_id = if_else(sentido == 'V','1','0')) %>% 
  select(-c(sentido)) %>% 
  mutate(letras = stringr::str_extract(trip_short_name, "[A-Z]+" ),
         numero = stringr::str_extract(trip_short_name, "[0-9]+" )) %>% 
  tidyr::unite(.,trip_short_name,letras,numero, na.rm = T,sep='')

gtfs <- read_gtfs(paste0("./dados/gtfs/",ano,"/gtfs_",ano,'-',mes_gtfs,'-',quinzena_gtfs,'Q.zip'))

trips <- gtfs$trips %>% 
  distinct(trip_short_name,direction_id,.keep_all = T) %>% 
  select(trip_short_name,direction_id,trip_headsign) %>% 
  mutate(trip_headsign = gsub('"','',trip_headsign)) %>% 
  mutate(direction_id = as.character(direction_id)) %>% 
  mutate(letras = stringr::str_extract(trip_short_name, "[A-Z]+" ),
         numero = stringr::str_extract(trip_short_name, "[0-9]+" )) %>% 
  tidyr::unite(.,trip_short_name,letras,numero, na.rm = T,sep='') %>% 
  mutate(trip_headsign = case_when(trip_short_name == 'SP852' & direction_id == '0' ~ 'Pedra de Guaratiba',
                                   trip_short_name == 'SP852' & direction_id == '1' ~ 'Campo Grande',
                                   TRUE ~ trip_headsign))

quadro <- quadro %>% 
  left_join(trips) %>% 
  select(-c(direction_id))

while (length(unique(quadro$teste))!=1){
  quadro <- quadro %>%
    group_by(trip_short_name,trip_headsign) %>%
    mutate(end_time = if_else(lead(headway_secs)==headway_secs & !is.na(lead(headway_secs)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = headway_secs==lead(headway_secs)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste)) %>%
    mutate(ordem = 1:n())
}

quadro <- quadro %>%
  ungroup() %>%
  mutate(trip_id = '') %>%
  select(trip_id,trip_headsign,trip_short_name,start_time,end_time,headway_secs) %>% 
  group_by(trip_short_name,trip_headsign) %>% 
  mutate(start_time = if_else(hour(hms(start_time)) < 2,
                              hms(start_time)+lubridate::hours(24),
                              hms(start_time))) %>% 
  mutate(start_time = if_else(hour(hms(start_time)) < head(hour(hms(start_time))),
                            hms(start_time)+lubridate::hours(24),
                            hms(start_time))) %>% 
  mutate(end_time = if_else(hour(hms(end_time)) < head(hour(hms(start_time))),
                            hms(end_time)+lubridate::hours(24),
                            hms(end_time))) %>% 
  mutate(start_time = paste(sprintf("%02d", hour(start_time)),
                            sprintf("%02d", minute(start_time)),
                            sprintf("%02d", second(start_time)),sep=':'),
         end_time = paste(sprintf("%02d", hour(end_time)),
                            sprintf("%02d", minute(end_time)),
                            sprintf("%02d", second(end_time)),sep=':'))

linhas <- quadro %>%
  select(trip_short_name,trip_headsign) %>%
  distinct_all()

servicos <- linhas$trip_short_name
vistas <- linhas$trip_headsign
qt <- c(1:nrow(linhas))

pasta_qh <- paste0("./resultados/quadro_horario/",ano,"/",mes_gtfs,"/qh_por_linha/")
ifelse(!dir.exists(file.path(getwd(),pasta_qh)), dir.create(file.path(getwd(),pasta_qh)), FALSE)
pasta_qh <- paste0("./resultados/quadro_horario/",ano,"/",mes_gtfs,"/qh_por_linha/",tipo_dia,"/")
ifelse(!dir.exists(file.path(getwd(),pasta_qh)), dir.create(file.path(getwd(),pasta_qh)), FALSE)

separarQuadros <- function(servicos,vistas){
  a <- quadro %>%
    filter(trip_short_name == servicos) %>%
    filter(trip_headsign == vistas)

  nome_arq <- paste0("qh_",servicos,"-",vistas,".csv")
  
  espaco <- data.frame(trip_id = '')
  
  quadro_final <- data.frame(tipo_dia) %>% 
    mutate(tipo_dia = case_when(tipo_dia == 'du' ~ 'DIA UTIL',
                                tipo_dia == 'sab' ~ 'SABADO',
                                tipo_dia == 'dom' ~ 'DOMINGO',
                                TRUE ~ tipo_dia))
  
  quadro_final <- quadro_final %>% 
    bind_rows(quadro_final) %>% 
    bind_rows(quadro_final) %>% 
    rename(trip_id = tipo_dia) %>% 
    bind_rows(espaco) %>% 
    bind_rows(espaco) %>% 
    bind_rows(a)
    

  fwrite(a,paste0(pasta_qh,nome_arq), quote=F)
}

mapply(separarQuadros,servicos,vistas)

