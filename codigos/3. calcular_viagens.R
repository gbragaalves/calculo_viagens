# Inicializacao ----

## Define parametros iniciais ----

ano <- '2023'
mes <- '03' ### mes dos dados de viagens e demanda
ano_gtfs <- '2023'
mes_gtfs <- '05'
quinzena_gtfs <- '01'
tipo_dia <- 'du'  ### mudar para DU
fator_tolerancia <- 1.5
dia_num <- ifelse(tipo_dia == 'sab',7,1)
dia_num <- ifelse(tipo_dia == 'du',NA,dia_num)


## Carrega bibliotecas e inicializa funcoes ----

library(sf)
library(dplyr)
library(Hmisc)
library(data.table)
library(tidyverse)
library(bizdays)
library(gtfstools)
library(lubridate)

bizdays::load_calendar("./dados/calendario.json")

`%not_like%` <- purrr::negate(`%like%`)

match_fun_left <- function(v1, v2) {
  ret <- data.frame(include = (v1 >= v2 - 600))
  ret
}

match_fun_right <- function(v1, v2) {
  ret <- data.frame(include = (v1 < v2 + 600))
  ret
}

## Carrega dados ----

linhas_usar <-
  fread("./insumos/2023/2023_05_du.csv", encoding = "UTF-8") %>% ### definir local do csv que vem do Google Sheets
  mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
         numero = stringr::str_extract(servico, "[0-9]+" )) %>% 
  unite(.,servico,letras,numero, na.rm = T,sep='') %>%
  rename(
         horario_inicio = inicio,
         horario_fim = fim,
         intervalo = pico
  ) %>%
  mutate(intervalo = gsub(',','.',intervalo),
         intervalo = as.numeric(intervalo),
         intervalo = ifelse(intervalo == 0,NA,intervalo),
         servico = as.character(servico),
         intervalo = ifelse(intervalo > 120,120,intervalo)) %>%
  select(servico,
         intervalo,
         viagem_inicial,
         viagem_final,
         horario_inicio,
         horario_fim,
         grupo_linha,
         rodar)

linhas_base <-
  fread("./insumos/2023/linhas_2023.csv", encoding = "UTF-8") %>% ### definir local do csv que vem do Google Sheets
  mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
         numero = stringr::str_extract(servico, "[0-9]+" )) %>%
  unite(.,servico,letras,numero, na.rm = T,sep='') %>%
  mutate(servico = as.character(servico)) %>%
  select(servico,
         viagem_inicial,
         viagem_final,
         grupo_linha) %>% 
  filter(servico %nin% linhas_usar)

linhas_bruto <- rbindlist(list(linhas_usar,linhas_base),fill = T)

linhas <- linhas_usar %>% 
  filter(servico %in% c('928')) %>% 
  #filter(servico == '301') %>% 
  #filter(rodar) %>% 
  #filter(!repetir_du) %>% 
  #filter(servico %in% frescao$servico) %>% 
  #mutate_all(., list(~na_if(.,""))) %>% 
  mutate(
    horario_inicio = as.POSIXct(horario_inicio, format = "%H:%M:%S", origin =
                                  "1970-01-01"),
    horario_fim = as.POSIXct(horario_fim, format = "%H:%M:%S", origin =
                               "1970-01-01")
  ) %>%
  mutate(horario_inicio = if_else(as.ITime(horario_inicio) < as.ITime("02:00:00"),
                                  horario_inicio + 86400,
                                  horario_inicio)) %>%
  mutate(horario_fim = if_else(horario_fim < horario_inicio,
                               horario_fim + 86400,
                               horario_fim)) %>%
  mutate(horario_inicio = lubridate::force_tz(horario_inicio, "America/Sao_Paulo"),
         horario_fim = lubridate::force_tz(horario_fim, "America/Sao_Paulo")) %>%
  drop_na() %>% 
  arrange(servico) %>% 
  mutate(modelo_espelhado = if_else(viagem_inicial %in% c('ambos','circular'),T,F))

end_sumario_detalhado <- paste0("./dados/sumarios/",ano,"/sumario_sent_hora_",mes,"_",tipo_dia,".csv")

sumario_trips <- fread(end_sumario_detalhado) %>% 
  rename(servico = servico_realizado) %>% 
  filter(servico %in% linhas_bruto$servico) %>% 
  left_join(select(linhas_bruto,servico,grupo_linha))

velocidade_grupos <- sumario_trips %>%
  group_by(grupo_linha,hora_partida) %>%
  summarise(velocidade_media = round(mean(velocidade_media),2))

gtfs <- read_gtfs(paste0("./dados/gtfs/",ano_gtfs,"/gtfs_",ano_gtfs,'-',mes_gtfs,'-',quinzena_gtfs,'Q.zip'))
gtfs <- filter_by_service_id(gtfs,'U')

routes <- gtfs$routes %>%
  mutate(letras = stringr::str_extract(route_short_name, "[A-Z]+" ),
         numero = stringr::str_extract(route_short_name, "[0-9]+" )) %>% 
  unite(.,route_short_name,letras,numero, na.rm = T,sep='') %>% 
  filter(route_short_name %in% linhas_bruto$servico) %>% 
  select(route_id) %>% 
  unlist()

gtfs <- filter_by_route_id(gtfs,routes)

trips <- gtfs$trips %>%
  mutate(letras = stringr::str_extract(trip_short_name, "[A-Z]+" ),
         numero = stringr::str_extract(trip_short_name, "[0-9]+" )) %>% 
  unite(.,trip_short_name,letras,numero, na.rm = T,sep='') %>% 
  group_by(route_id) %>%
  ungroup() %>% 
  distinct(shape_id,trip_short_name,.keep_all = T) %>% 
  select(trip_id,trip_short_name,shape_id,direction_id)

gtfs <- filter_by_trip_id(gtfs,trips$trip_id)

gtfs$shapes <- as.data.table(gtfs$shapes) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes <- convert_shapes_to_sf(gtfs) %>% 
  mutate(extensao = round(as.numeric(st_length(.)),0)) %>% 
  st_drop_geometry()

trips <- trips %>% 
  left_join(shapes) %>% 
  select(-c(trip_id))

trips_resultado <- trips %>% 
  rename(servico = trip_short_name,
         direcao = direction_id) %>% 
  mutate(direcao = if_else(direcao == '1','V','I'),
         extensao = round(extensao,0)) %>% 
  select(-c(shape_id))

pasta_ano <- paste0("./resultados/trips/",ano_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_ano)), dir.create(file.path(getwd(),pasta_ano)), FALSE)
pasta_mes <- paste0("./resultados/trips/",ano_gtfs,"/",mes_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_mes)), dir.create(file.path(getwd(),pasta_mes)), FALSE)

fwrite(trips_resultado,paste0(pasta_mes,"/trips_",ano,"-",mes_gtfs,"-",quinzena_gtfs,"Q_",tipo_dia,".csv"), sep=";",dec=",")

rm(gtfs,routes,shapes,trips_resultado)

# Processamento ----

trips <- trips %>%
  rename(servico = trip_short_name) %>%
  left_join(select(
    linhas,
    servico,
    viagem_inicial,
  ),
  by = "servico") %>% 
  mutate(sentido = if_else(viagem_inicial == 'circular','C',
                           if_else(direction_id == 0,'I','V'))) %>% 
  select(-c(direction_id,shape_id)) %>% 
  arrange(servico,sentido)

linhas_trips <- trips %>%
  select(-c(viagem_inicial)) %>% 
  left_join(.,linhas, by = "servico") %>% 
  drop_na() %>% 
  mutate(grupo_linha = if_else(grupo_linha == 'Auxiliar AP 51' & 'Auxiliar AP 51' %nin% sumario_trips$grupo_linha,'Auxiliar AP 52',grupo_linha))

rho <- readRDS(paste0("./dados/demanda/",ano,"/rho_",ano,"_",mes,".rds")) %>%
  ungroup()

demanda_linha <- rho %>%
  filter(if (tipo_dia == 'du') is.bizday(data,"Rio_Janeiro") 
         else lubridate::wday(data) == dia_num) %>%
  group_by(servico,hora) %>%
  summarise(pax = round(mean(demanda))) %>%
  ungroup() %>%
  group_by(servico) %>%
  mutate(perc_pax = round((pax/max(pax))*100,2))

demanda_grupo <- demanda_linha %>%
  left_join(select(linhas_bruto,servico,grupo_linha)) %>%
  group_by(grupo_linha,hora) %>%
  summarise(pax_grupo = sum(pax)) %>%
  ungroup() %>%
  group_by(grupo_linha) %>%
  mutate(perc_pax_grupo = round((pax_grupo/max(pax_grupo))*100,2))

rm(rho)

freq_bruta <- linhas_trips %>%
  left_join(demanda_grupo) %>%
  left_join(demanda_linha) %>%
  mutate(perc_hora_final = if_else(is.na(perc_pax),perc_pax_grupo,perc_pax),
         hora = as.integer(hora)) %>%
  select(-c(perc_pax,pax_grupo,perc_pax_grupo)) %>%
  ungroup() %>%
  left_join(select(sumario_trips,servico,sentido,hora_partida,velocidade_media),
            by=c("servico"="servico","sentido"="sentido","hora"="hora_partida")) %>%
  rename(veloc_media_linha = velocidade_media) %>%
  left_join(select(velocidade_grupos,grupo_linha,hora_partida,velocidade_media),
            by=c("grupo_linha"="grupo_linha","hora"="hora_partida")) %>%
  rename(veloc_media_grupo = velocidade_media) %>%
  group_by(servico,sentido) %>%
  arrange(hora) %>%
  mutate(velocidade_media = if_else(is.na(veloc_media_linha),veloc_media_grupo,veloc_media_linha),
         velocidade_media = ifelse(is.na(velocidade_media),max(velocidade_media, na.rm = T),velocidade_media)) %>%
  ungroup() %>%
  select(-c(grupo_linha,veloc_media_linha,veloc_media_grupo)) %>%
  mutate(hora = as.POSIXct(paste0(hora,":00:00"), format="%H:%M:%S")) %>%
  group_by(servico,sentido) %>%
  mutate(start_time = case_when(row_number() == 1 & hora>horario_inicio ~ horario_inicio,
                                hora>horario_fim ~ horario_fim,
                                TRUE ~ hora)) %>%
  mutate(end_time = hora+3600) %>%
  mutate(end_time = case_when(row_number() == n() & end_time<horario_fim ~ horario_fim,
                              hora>horario_fim ~ horario_fim,
                              TRUE ~ end_time)) %>%
  arrange(servico,sentido) %>%
  ungroup()

freq_bruta_inicio <- freq_bruta %>%
  group_by(servico,sentido) %>%
  mutate(end_time = end_time - 2400,
         velocidade_media = if_else(row_number() != 1, (velocidade_media+lag(velocidade_media))/2,velocidade_media)) %>%
  ungroup()


freq_bruta_fim <- freq_bruta %>%
  group_by(servico,sentido) %>%
  mutate(start_time = start_time + 2400,
         perc_hora_final = if_else(row_number() != n(), (perc_hora_final+(lead(perc_hora_final)*2))/3,perc_hora_final),
         velocidade_media = if_else(row_number() != n(), (velocidade_media+lead(velocidade_media))/2,velocidade_media)) %>%
  ungroup()


freq_bruta_meio <- freq_bruta %>%
  mutate(start_time = start_time + 1200,
         end_time = end_time - 1200)

freq_bruta_proc <- rbindlist(list(freq_bruta_inicio,freq_bruta_meio,freq_bruta_fim)) %>%
  group_by(servico) %>% 
  mutate(hora_corte = horario_inicio + difftime(horario_fim,horario_inicio,units = "mins")/2) %>% 
  group_by(servico,sentido) %>%
  arrange(servico,sentido,start_time) %>%
  mutate(start_time = if_else(start_time < horario_inicio,horario_inicio,start_time),
         end_time = if_else(end_time > horario_fim,horario_fim,end_time))  %>%
  filter(start_time != end_time,
         end_time > start_time,
         start_time < end_time) %>%
  mutate(perc_hora_final = case_when(row_number() == n() ~ perc_hora_final,
                                     TRUE ~ (perc_hora_final+lead(perc_hora_final))/2),
         velocidade_media = case_when(row_number() == 1 ~ (velocidade_media+lead(velocidade_media))/2,
                                      row_number() == n() ~ (velocidade_media+lag(velocidade_media))/2,
                                      TRUE ~ (velocidade_media+lag(velocidade_media)+lead(velocidade_media))/3))%>%
  mutate(tempo_viagem = extensao/(velocidade_media/3.6)) %>%
  ungroup() %>%
  group_by(servico) %>%
  mutate(hora_usar = min(start_time[velocidade_media == min(velocidade_media[hora == min(hora[perc_hora_final == max(perc_hora_final)])])])) %>%
  mutate(perc_hora_final = if_else(perc_hora_final > 80,perc_hora_final/0.9,perc_hora_final/0.85), #taxa de conforto
         perc_hora_final = ifelse(perc_hora_final > 100, 100, perc_hora_final)) %>%
  mutate(frota_referencia = round((sum(tempo_viagem[start_time==hora_usar])+
                                     (28 * (1.25-(max(perc_hora_final[start_time==hora_usar])/100))*60))/(intervalo*60))) %>%
  mutate(frota_referencia = ifelse(frota_referencia < 1,1,frota_referencia)) %>%
  rename(intervalo_teorico = intervalo) %>%
  mutate(intervalo_pratico = round((sum(tempo_viagem[start_time==hora_usar])+
                                      (28 * (1.25-(max(perc_hora_final[start_time==hora_usar])/100))*60))/frota_referencia)) %>%
  mutate(intervalo_pratico = if_else((intervalo_pratico/60)*fator_tolerancia > intervalo_teorico,intervalo_teorico*60,intervalo_pratico)) %>%  ### impedir intervalo menor que referência
  mutate(intervalo_pratico = case_when(as.numeric(intervalo_pratico) <= 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 30),
                                       as.numeric(intervalo_pratico) > 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 60),
                                       is.na(intervalo_pratico) ~ as.numeric(intervalo_pratico))) %>%
  mutate(intervalo_pratico_mins = intervalo_pratico/60) %>%
  arrange(servico,sentido,start_time) %>%
  mutate(viagem_inicial = case_when(viagem_inicial == 'ida' ~ 'I',
                                    viagem_inicial == 'volta' ~ 'V',
                                    viagem_inicial == 'circular' ~ 'C',
                                    viagem_inicial == 'ambos' ~ sentido,
                                    TRUE ~ viagem_inicial),
         viagem_final = case_when(viagem_final == 'ida' ~ 'I',
                                  viagem_final == 'volta' ~ 'V',
                                  viagem_final == 'circular' ~'C',
                                  viagem_final == 'ambos' ~ sentido,
                                  TRUE ~ viagem_final)) %>%
  mutate(inicio_operacao = horario_inicio) %>% 
  mutate(horario_inicio = if (unique(modelo_espelhado)==T) horario_inicio else
    if_else((sentido == 'I' & viagem_inicial == 'V') | (sentido == 'V' & viagem_inicial == 'I'),
            lubridate::ceiling_date(horario_inicio
                                  +420+head(tempo_viagem[sentido==viagem_inicial&end_time>horario_inicio],1),unit = "5 minutes"),
            horario_inicio),
    horario_fim = if (unique(modelo_espelhado)==T) horario_fim else
      if_else((sentido == 'I' & viagem_final == 'V') | (sentido == 'V' & viagem_final == 'I'),
              lubridate::ceiling_date((horario_fim-tail(tempo_viagem[sentido==viagem_final&start_time<horario_fim],1)-420),unit = "5 minutes"), ## rever uso de tempo de placa
              horario_fim)) %>%
  ungroup() %>%
  group_by(servico,sentido) %>%
  mutate(start_time = if_else(start_time < horario_inicio,horario_inicio,start_time),
         end_time = if_else(end_time > horario_fim,horario_fim,end_time))  %>%
  filter(start_time != end_time,
         end_time > start_time,
         start_time < end_time) %>%
  arrange(servico,sentido,start_time) %>%
  rename(intervalo_pico = intervalo_pratico,
         perc_pax = perc_hora_final) %>%
  ungroup() %>%
  group_by(servico,start_time) %>%
  mutate(tempo_ciclo = sum(tempo_viagem)+(28 * (1.25-(perc_pax/100))*60),
         tempo_placa = 28 * (1.25-(perc_pax/100))) %>%
  mutate(intervalo_hora_teorico = intervalo_pico/(perc_pax/100),
         intervalo_hora_teorico = case_when(as.numeric(intervalo_hora_teorico) <= 300 ~ plyr::round_any(as.numeric(intervalo_hora_teorico), 30),
                                            as.numeric(intervalo_hora_teorico) > 300 ~ plyr::round_any(as.numeric(intervalo_hora_teorico), 60),
                                            is.na(intervalo_hora_teorico) ~ as.numeric(intervalo_hora_teorico)),
         intervalo_hora_teorico = if_else(intervalo_hora_teorico < intervalo_pico,intervalo_pico,intervalo_hora_teorico),
         intervalo_hora_teorico = case_when(intervalo_pico<=300 & intervalo_hora_teorico > 600 & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) ~ 600,
                                            intervalo_pico<=300 & intervalo_hora_teorico > 900 & (start_time < (horario_inicio+3600) | start_time > (horario_fim-3600)) ~ 900,
                                            intervalo_pico>300 & intervalo_pico<=600 & intervalo_hora_teorico > 1200 & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) ~ 1200,
                                            intervalo_pico>300 & intervalo_pico<=600 & intervalo_hora_teorico > 1800 & (start_time < (horario_inicio+3600) | start_time > (horario_fim-3600)) ~ 1800,
                                            intervalo_pico>600 & intervalo_pico<=900&intervalo_hora_teorico > 1800 & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600)  ~ 1800,
                                            intervalo_pico>600 & intervalo_pico<=900&intervalo_hora_teorico > 2700 & (start_time < (horario_inicio+3600) | start_time > (horario_fim-3600)) ~ 2700,
                                            intervalo_pico>900 & intervalo_pico<=1800&intervalo_hora_teorico > 2700 & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600)  ~ 2700,
                                            intervalo_pico<=3600&intervalo_hora_teorico > 3600 ~ 3600,
                                            intervalo_hora_teorico > 4500 ~ 4500,
                                            TRUE ~ intervalo_hora_teorico),
         frota_hora_dec = tempo_ciclo/intervalo_hora_teorico,
         frota_hora = round(frota_hora_dec),
         frota_hora = if_else(frota_hora<1,1,frota_hora),
         frota_hora = if_else(frota_hora>frota_referencia,frota_referencia,frota_hora),
         intervalo_pratico = round((sum(tempo_viagem)+
                                      (28 * (1.25-(perc_pax/100))*60))/frota_hora),
         intervalo_pratico = if_else(intervalo_pratico < intervalo_pico,intervalo_pico,intervalo_pratico),
         frota_hora = case_when(intervalo_pico<=300 & intervalo_pratico > (600*fator_tolerancia) & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) & frota_hora < frota_referencia ~ frota_hora+1,
                                intervalo_pico>300 & intervalo_pico<=600 & intervalo_pratico > (1200*fator_tolerancia) & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) & frota_hora < frota_referencia ~ frota_hora+1,
                                intervalo_pico>600 & intervalo_pico<=900&intervalo_pratico > (1800*fator_tolerancia) & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) & frota_hora < frota_referencia ~ frota_hora+1,
                                intervalo_pico>900 & intervalo_pico<=1800&intervalo_pratico > (2700*fator_tolerancia) & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) & frota_hora < frota_referencia ~ frota_hora+1,
                                intervalo_pico>1800 & intervalo_pico<=2700&intervalo_pratico >  (3600*fator_tolerancia) & start_time > (horario_inicio+3600) & start_time < (horario_fim-3600) & frota_hora < frota_referencia ~ frota_hora+1,
                                intervalo_pratico > (3600*fator_tolerancia) & frota_hora < frota_referencia ~ frota_hora+1,
                                TRUE ~ frota_hora),
         intervalo_pratico = round((sum(tempo_viagem)+
                                      (28 * (1.25-(perc_pax/100))*60))/frota_hora),
         intervalo_pratico = if_else(intervalo_pratico < intervalo_pico,intervalo_pico,intervalo_pratico),
         intervalo_pratico = case_when(as.numeric(intervalo_pratico) <= 300 ~ plyr::round_any(as.numeric(intervalo_pratico), 30),
                                       as.numeric(intervalo_pratico) > 300 ~ plyr::round_any(as.numeric(intervalo_pratico), 60),
                                       is.na(intervalo_pratico) ~ as.numeric(intervalo_pratico))
  ) %>%
  ungroup() %>%
  group_by(servico) %>%
  mutate(frota_pico_manha = max(frota_hora[start_time<hora_corte]),
         frota_pico_tarde = max(frota_hora[start_time>hora_corte]),
         inicio_pico_manha = min(start_time[frota_hora == frota_pico_manha & sentido == viagem_inicial], na.rm = T),
         fim_pico_manha = if_else(unique(frota_referencia) > 1,min(start_time[frota_hora == frota_pico_manha & frota_hora > lead(frota_hora) & sentido == viagem_inicial], na.rm = T),head(horario_fim[sentido == viagem_inicial],1)),
         fim_pico_tarde = if_else(unique(frota_referencia) > 1,max(end_time[frota_hora == frota_pico_tarde & frota_hora > lead(frota_hora) & sentido == viagem_final], na.rm = T),tail(horario_fim[sentido == viagem_final],1))) %>% 
  ungroup() %>% 
  group_by(servico,sentido) %>% 
  mutate(fim_pico_manha = if_else(is.na(fim_pico_manha),horario_fim,fim_pico_manha),
         fim_pico_manha = if_else(is.infinite(fim_pico_manha),horario_fim,fim_pico_manha),
         fim_pico_tarde = if_else(is.na(fim_pico_tarde),horario_fim,fim_pico_tarde),
         fim_pico_tarde = if_else(is.infinite(fim_pico_tarde),horario_fim,fim_pico_tarde)) %>%
  mutate(start_time = lubridate::force_tz(start_time, "America/Sao_Paulo"),
         end_time = lubridate::force_tz(end_time, "America/Sao_Paulo"),
         horario_inicio = lubridate::force_tz(horario_inicio, "America/Sao_Paulo"),
         horario_fim = lubridate::force_tz(horario_fim, "America/Sao_Paulo"),
         inicio_pico_manha = lubridate::force_tz(inicio_pico_manha, "America/Sao_Paulo"),
         fim_pico_manha = lubridate::force_tz(fim_pico_manha, "America/Sao_Paulo"),
         fim_pico_tarde = lubridate::force_tz(fim_pico_tarde, "America/Sao_Paulo")) %>%
  ungroup() %>% 
  group_by(servico) %>% 
  mutate(entrega_pico_manha = if (unique(modelo_espelhado)==T) as.POSIXct(as.ITime("00:00:00"))
         else inicio_pico_manha + tempo_viagem[start_time==inicio_pico_manha & sentido == viagem_inicial],
         entrega_pico_tarde = if (unique(modelo_espelhado)==T) as.POSIXct(as.ITime("00:00:00"))
         else fim_pico_tarde - tempo_viagem[end_time==fim_pico_tarde & sentido == viagem_final]) %>%
  mutate(entrega_pico_manha = lubridate::force_tz(entrega_pico_manha, "America/Sao_Paulo"),
         entrega_pico_tarde = lubridate::force_tz(entrega_pico_tarde, "America/Sao_Paulo")) %>%
  ungroup() %>%
  group_by(servico,sentido) %>%
  mutate(intervalo_pratico = if_else(lag(intervalo_pratico)==lead(intervalo_pratico) &
                                       !is.na(lag(intervalo_pratico)) &
                                       !is.na(lead(intervalo_pratico)),lag(intervalo_pratico),intervalo_pratico),
         intervalo_pratico_min = intervalo_pratico/60,
  ) %>%
  ungroup() %>%
  select(-c(frota_pico_manha,frota_pico_tarde,hora,pax,hora_usar,intervalo_pratico_mins)) %>%
  select(servico,sentido,start_time,end_time,frota_hora,intervalo_pratico_min,perc_pax,everything())

## Manha ----

### Ida ----

freq_ida_manha <- freq_bruta_proc %>%
  filter(sentido == viagem_inicial,
         modelo_espelhado == F,
         start_time < hora_corte)

for (z in 1:5){
  freq_ida_manha <- freq_ida_manha %>%
    group_by(servico) %>%
    mutate(end_time = if_else(intervalo_pratico > difftime(end_time,start_time, units = "secs") &
                                start_time < horario_inicio+3600,start_time+intervalo_pratico,end_time)) %>%
    mutate(start_time = if_else(row_number() != 1,lag(end_time),start_time)) %>%
    #mutate(teste = intervalo_pratico <= difftime(end_time,start_time, units = "secs")) %>%
    ungroup()
}

while (length(unique(freq_ida_manha$teste))!=1){
  freq_ida_manha <- freq_ida_manha %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_ida_manha <- freq_ida_manha %>%
  ungroup() %>%
  select(-teste) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE),
         minutos = as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "mins")))  %>%
  mutate(end_time_original = end_time) %>%
  select(servico,sentido,start_time,end_time,end_time_original,frota_hora,intervalo_pratico_min,perc_pax,everything())


while (all(freq_ida_manha$inteiro)!=TRUE){
  freq_ida_manha <- freq_ida_manha %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(
      inteiro,end_time,
      if_else((lead(intervalo_pratico)<intervalo_pratico | is.na(lead(intervalo_pratico)))&end_time<fim_pico_manha,
              start_time+as.integer(minutos/(intervalo_pratico/60))*intervalo_pratico,
              start_time+as.integer(minutos/(intervalo_pratico/60)+1)*intervalo_pratico))) %>%
    mutate(end_time = as.POSIXct(end_time, origin='1970-01-01')) %>%
    ungroup() %>%
    arrange(servico,sentido,start_time)
  
  freq_ida_manha <- freq_ida_manha %>%
    group_by(servico,sentido) %>%
    filter(row_number() == 1 | row_number() == n() | difftime(end_time,start_time,units = "secs")>intervalo_pratico) %>%
    mutate(start_time=if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
    mutate(start_time = as.POSIXct(start_time, origin='1970-01-01')) %>%
    ungroup() %>%
    filter(start_time < horario_fim) %>%
    arrange(servico,sentido,start_time)
  
  freq_ida_manha <- freq_ida_manha %>%
    group_by(servico,sentido) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(difftime(as.POSIXct(end_time_original), as.POSIXct(start_time), units = "mins"))) %>%
    ungroup() %>%
    filter(start_time != end_time) %>%
    filter(viagens > 0) %>% 
    arrange(servico,sentido,start_time)
}

freq_ida_manha <- freq_ida_manha %>%
  filter(start_time < hora_corte)

### Volta ----

freq_volta_manha <- freq_bruta_proc %>%
  filter(sentido != viagem_inicial,
         modelo_espelhado == F,
         start_time < hora_corte)

linhas_volta_manha <- unique(freq_volta_manha$servico)

dados_ida <- freq_ida_manha %>%
  ungroup() %>%
  select(servico,start_time,end_time,viagens,tempo_viagem) %>%
  mutate(start_time = start_time+tempo_viagem) %>%
  group_by(servico) %>% 
  mutate(end_time = if_else(!is.na(lead(start_time)),lead(start_time),end_time)) %>%
  mutate(viagens_acum = cumsum(viagens)) %>% 
  rename(start_time_ida = start_time,
         end_time_ida = end_time,
         viagens_ida = viagens,
         servico_ida = servico,
         tempo_viagem_ida = tempo_viagem) %>%
  filter(end_time_ida > start_time_ida)

freq_volta_manha <- freq_volta_manha %>%
  fuzzyjoin::fuzzy_left_join(dados_ida, by = c("servico" = "servico_ida",
                                               "start_time"= "start_time_ida", "end_time" = "end_time_ida"),
                             match_fun = list(`==`, match_fun_left, match_fun_right))

freq_volta_manha <- freq_volta_manha %>%
  mutate(frota_hora = if_else((start_time < entrega_pico_manha & !is.na(viagens_ida)), viagens_ida,frota_hora)) %>% 
  group_by(servico) %>% 
  mutate(frota_hora = if_else((row_number() == 1 & !is.na(viagens_acum)), viagens_acum,frota_hora))

for (z in 1:20){
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico) %>%
    mutate(end_time = if_else(lead(frota_hora)==frota_hora & !is.na(lead(frota_hora)) & start_time < entrega_pico_manha,lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = frota_hora==lead(frota_hora)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_volta_manha <- freq_volta_manha %>%
  select(-c(teste))

while (length(unique(freq_volta_manha$teste))!=1){
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico))& start_time >= entrega_pico_manha,lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico) & start_time >= entrega_pico_manha) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_volta_manha <- freq_volta_manha %>%
  group_by(servico) %>%
  mutate(intervalo_frota = if_else(start_time < entrega_pico_manha,
                                   tempo_viagem/frota_hora,
                                   intervalo_pratico),
         intervalo_pratico = if_else(intervalo_frota > intervalo_pratico, intervalo_frota, intervalo_pratico),
         intervalo_pratico = case_when(as.numeric(intervalo_pratico) <= 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 15),
                                       as.numeric(intervalo_pratico) > 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 60),
                                       is.na(intervalo_pratico) ~ as.numeric(intervalo_pratico)),
         intervalo_pratico_min = intervalo_pratico/60) %>%
  ungroup()

freq_volta_manha <- freq_volta_manha %>%
  select(-c(teste)) %>% 
  group_by(servico) %>%
  mutate(start_time = if_else(row_number()== 1,inicio_operacao+tempo_viagem_ida,start_time),
         start_time = lubridate::ceiling_date(start_time,unit="5 minutes")) %>% 
  mutate(end_time = if_else(row_number() == 1 & 
                              lubridate::ceiling_date(inicio_operacao+tempo_viagem_ida,unit="5 minutes") != start_time
                              ,inicio_operacao+tempo_viagem_ida,end_time),
         end_time = lubridate::ceiling_date(end_time,unit="5 minutes")) %>% 
  mutate(start_time = if_else(end_time < start_time,end_time,start_time)) %>% 
  mutate(start_time = if_else(lag(end_time) < start_time & row_number() != 1,lag(end_time),start_time)) %>% 
  filter(start_time != end_time) %>% 
  mutate(ip = ifelse(start_time < entrega_pico_manha,
                     plyr::round_any(as.integer(difftime(end_time,start_time,units='secs')/frota_hora),60)
                     ,intervalo_pratico),
         intervalo_pratico = ifelse(start_time < entrega_pico_manha & ip > intervalo_pratico &
                                      frota_referencia > 2 & ip != 0,plyr::round_any(as.numeric(ip), 60),intervalo_pratico),
         intervalo_pratico = ifelse(row_number() == 1 &
                                      frota_referencia > 2 & ip != 0,plyr::round_any(as.numeric(ip), 60),intervalo_pratico),
         intervalo_pratico_min = intervalo_pratico/60) %>% 
  mutate(end_time = if_else(frota_hora == 1 & row_number() == 1,
                            start_time + intervalo_pratico,
                            end_time)) %>% 
  mutate(start_time = if_else(lag(end_time)>start_time & !is.na(lag(end_time)),lag(end_time),start_time))


while (length(unique(freq_volta_manha$teste))!=1){
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico) %>%
    mutate(end_time = if_else(intervalo_pratico > difftime(end_time,start_time, units = "secs") & row_number() != 1,start_time+intervalo_pratico,end_time)) %>%
    mutate(start_time = if_else(row_number() != 1,lag(end_time),start_time)) %>%
    filter(start_time < horario_fim) %>% 
    mutate(teste = intervalo_pratico <= difftime(end_time,start_time, units = "secs")) %>%
    mutate(teste = if_else(row_number() == 1,T,teste)) %>% 
    ungroup()
}

freq_volta_manha <- freq_volta_manha %>%
  select(-c(teste))

while (length(unique(freq_volta_manha$teste))!=1){
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_volta_manha <- freq_volta_manha %>%
  ungroup() %>%
  select(-teste) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE),
         minutos = as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "mins")))  %>%
  mutate(end_time_original = end_time) %>%
  select(servico,sentido,start_time,end_time,end_time_original,frota_hora,intervalo_pratico_min,perc_pax,everything())

while (all(freq_volta_manha$inteiro)!=TRUE){
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(
      inteiro | row_number() == 1,end_time,
      if_else((lead(intervalo_pratico)<intervalo_pratico | is.na(lead(intervalo_pratico)))&end_time<fim_pico_manha,
              start_time+as.integer(minutos/(intervalo_pratico/60))*intervalo_pratico,
              start_time+as.integer(minutos/(intervalo_pratico/60)+1)*intervalo_pratico))) %>%
    mutate(end_time = as.POSIXct(end_time, origin='1970-01-01')) %>%
    ungroup() %>%
    arrange(servico,sentido,start_time)
  
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico,sentido) %>%
    filter(row_number() == 1 | row_number() == n() | difftime(end_time,start_time,units = "secs")>intervalo_pratico) %>%
    mutate(start_time=if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
    mutate(start_time = as.POSIXct(start_time, origin='1970-01-01')) %>%
    ungroup() %>%
    filter(start_time < horario_fim) %>%
    arrange(servico,sentido,start_time)
  
  freq_volta_manha <- freq_volta_manha %>%
    group_by(servico,sentido) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           viagens = ifelse(row_number() == 1,as.integer(viagens),viagens),
           viagens = ifelse(row_number() == 1 & viagens < 1,1,viagens),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(difftime(as.POSIXct(end_time_original), as.POSIXct(start_time), units = "mins"))) %>%
    ungroup() %>%
    filter(start_time != end_time) %>%
    arrange(servico,sentido,start_time)
}

lvmf <- unique(freq_volta_manha$servico)

if(length(linhas_volta_manha) != length(lvmf)){
  print('Erro! Rever cálculos.')
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
}

horarios_volta_tarde <- freq_volta_manha %>%
  select(servico,end_time,end_time_original) %>%
  group_by(servico) %>%
  slice_max(end_time) %>%
  mutate(end_time_original = if_else(end_time > end_time_original,end_time,end_time_original)) %>%
  rename(hora_corte_tarde = end_time_original,
         fim_manha = end_time) %>%
  ungroup()

## Tarde ----

### Volta ----

freq_volta_tarde <- freq_bruta_proc %>%
  filter(sentido != viagem_inicial,
         modelo_espelhado == F) %>%
  left_join(horarios_volta_tarde) %>%
  filter(start_time >= hora_corte_tarde) %>%
  group_by(servico) %>%
  mutate(start_time = if_else(row_number() == 1,fim_manha,start_time)) %>%
  ungroup() %>%
  select(-c(hora_corte_tarde,inicio_pico_manha,fim_pico_manha,entrega_pico_manha,fim_manha))

while (length(unique(freq_volta_tarde$teste))!=1){
  freq_volta_tarde <- freq_volta_tarde %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_volta_tarde <- freq_volta_tarde %>%
  ungroup() %>%
  select(-teste) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE),
         minutos = as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "mins")))  %>%
  mutate(end_time_original = end_time) %>%
  select(servico,sentido,start_time,end_time,end_time_original,frota_hora,intervalo_pratico_min,perc_pax,everything())


while (all(freq_volta_tarde$inteiro)!=TRUE){
  freq_volta_tarde <- freq_volta_tarde %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(
      inteiro,end_time,
      if_else((lead(intervalo_pratico)<intervalo_pratico | is.na(lead(intervalo_pratico)))&end_time<fim_pico_tarde,
              start_time+as.integer(minutos/(intervalo_pratico/60))*intervalo_pratico,
              start_time+as.integer(minutos/(intervalo_pratico/60)+1)*intervalo_pratico))) %>%
    mutate(end_time = as.POSIXct(end_time, origin='1970-01-01')) %>%
    ungroup() %>%
    arrange(servico,sentido,start_time)
  
  freq_volta_tarde <- freq_volta_tarde %>%
    group_by(servico,sentido) %>%
    filter(row_number() == 1 | row_number() == n() | difftime(end_time,start_time,units = "secs")>intervalo_pratico) %>%
    mutate(start_time=if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
    mutate(start_time = as.POSIXct(start_time, origin='1970-01-01')) %>%
    ungroup() %>%
    filter(start_time < horario_fim) %>%
    arrange(servico,sentido,start_time)
  
  freq_volta_tarde <- freq_volta_tarde %>%
    group_by(servico,sentido) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(difftime(as.POSIXct(end_time_original), as.POSIXct(start_time), units = "mins"))) %>%
    ungroup() %>%
    filter(start_time != end_time) %>%
    arrange(servico,sentido,start_time)
}

freq_volta_tarde <- freq_volta_tarde %>%
  group_by(servico,sentido) %>%
  arrange(end_time) %>%
  mutate(end_time = if_else(row_number() == n() & end_time < horario_fim,horario_fim,end_time)) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
  mutate(viagens_obj = ceiling(viagens)) %>%
  mutate(end_time = if_else(row_number() == n() & inteiro==F,start_time+(intervalo_pratico*viagens_obj),end_time)) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
  ungroup()

freq_volta <- rbindlist(list(freq_volta_manha,freq_volta_tarde), fill = T)

### Ida ----

horarios_ida_tarde <- freq_ida_manha %>%
  select(servico,end_time,end_time_original) %>%
  group_by(servico) %>%
  slice_max(end_time) %>%
  mutate(end_time_original = if_else(end_time > end_time_original,end_time,end_time_original)) %>%
  rename(hora_corte_tarde = end_time_original,
         fim_manha = end_time) %>%
  ungroup()

freq_ida_tarde <- freq_bruta_proc %>%
  filter(sentido == viagem_inicial,
         modelo_espelhado == F) %>%
  left_join(horarios_ida_tarde) %>%
  filter(start_time >= hora_corte_tarde) %>%
  group_by(servico) %>%
  mutate(start_time = if_else(row_number() == 1,fim_manha,start_time)) %>%
  ungroup() %>%
  select(-c(hora_corte_tarde,inicio_pico_manha,fim_pico_manha,entrega_pico_manha,fim_manha))

dados_volta <- freq_volta_tarde %>%
  ungroup() %>%
  select(servico,start_time,end_time,frota_hora,tempo_viagem) %>%
  mutate(end_time = end_time-tempo_viagem) %>%
  mutate(start_time = if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
  rename(start_time_volta = start_time,
         end_time_volta = end_time,
         frota_volta = frota_hora,
         servico_volta = servico) %>%
  select(-c(tempo_viagem)) %>%
  filter(end_time_volta > start_time_volta)

freq_ida_tarde <- freq_ida_tarde %>%
  fuzzyjoin::fuzzy_left_join(dados_volta, by = c("servico" = "servico_volta",
                                                 "start_time"= "start_time_volta", "end_time" = "end_time_volta"),
                             match_fun = list(`==`, match_fun_left, match_fun_right)) %>%
  group_by(servico) %>%
  mutate(frota_volta = if_else(is.na(frota_volta),max(frota_volta,na.rm = T),frota_volta)) %>%
  ungroup()

freq_ida_tarde <- freq_ida_tarde %>%
  mutate(frota_hora = if_else(start_time > entrega_pico_tarde & !is.na(entrega_pico_tarde), frota_volta,frota_hora))

while (length(unique(freq_ida_tarde$teste))!=1){
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_ida_tarde <- freq_ida_tarde %>%
  mutate(intervalo_frota = if_else(start_time > entrega_pico_tarde,
                                   tempo_viagem/frota_hora,
                                   intervalo_pratico),
         intervalo_pratico = if_else(intervalo_frota > intervalo_pratico, intervalo_frota, intervalo_pratico),
         intervalo_pratico = case_when(as.numeric(intervalo_pratico) <= 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 30),
                                       as.numeric(intervalo_pratico) > 900 ~ plyr::round_any(as.numeric(intervalo_pratico), 60),
                                       is.na(intervalo_pratico) ~ as.numeric(intervalo_pratico)),
         intervalo_pratico_min = intervalo_pratico/60) %>%
  ungroup()

while (length(unique(freq_ida_tarde$teste))!=1){
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico) %>%
    mutate(end_time = if_else(intervalo_pratico > difftime(end_time,start_time, units = "secs"),start_time+intervalo_pratico,end_time)) %>%
    mutate(start_time = if_else(row_number() != 1,lag(end_time),start_time)) %>%
    mutate(teste = intervalo_pratico <= difftime(end_time,start_time, units = "secs")) %>%
    ungroup()
}

freq_ida_tarde <- freq_ida_tarde %>%
  select(-c(teste))

while (length(unique(freq_ida_tarde$teste))!=1){
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
    filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
    mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
    mutate(teste = if_else(is.na(teste),FALSE,teste))
}

freq_ida_tarde <- freq_ida_tarde %>%
  ungroup() %>%
  select(-teste) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE),
         minutos = as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "mins")))  %>%
  mutate(end_time_original = end_time) %>%
  select(servico,sentido,start_time,end_time,end_time_original,frota_hora,intervalo_pratico_min,perc_pax,everything())


while (all(freq_ida_tarde$inteiro)!=TRUE){
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico,sentido) %>%
    mutate(end_time = if_else(
      inteiro,end_time,
      if_else((lead(intervalo_pratico)<intervalo_pratico | is.na(lead(intervalo_pratico)))&start_time<fim_pico_tarde,
              start_time+as.integer(minutos/(intervalo_pratico/60))*intervalo_pratico,
              start_time+as.integer(minutos/(intervalo_pratico/60)+1)*intervalo_pratico))) %>%
    mutate(end_time = as.POSIXct(end_time, origin='1970-01-01')) %>%
    ungroup() %>%
    arrange(servico,sentido,start_time)
  
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico,sentido) %>%
    filter(row_number() == 1 | row_number() == n() | difftime(end_time,start_time,units = "secs")>intervalo_pratico) %>%
    mutate(start_time=if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
    mutate(start_time = as.POSIXct(start_time, origin='1970-01-01')) %>%
    ungroup() %>%
    filter(start_time < horario_fim) %>%
    arrange(servico,sentido,start_time)
  
  freq_ida_tarde <- freq_ida_tarde %>%
    group_by(servico,sentido) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(difftime(as.POSIXct(end_time_original), as.POSIXct(start_time), units = "mins"))) %>%
    ungroup() %>%
    filter(start_time != end_time) %>%
    arrange(servico,sentido,start_time)
}


freq_ida_tarde <- freq_ida_tarde %>%
  group_by(servico,sentido) %>%
  arrange(end_time) %>%
  mutate(end_time = if_else(row_number() == n() & end_time < horario_fim,horario_fim,end_time)) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
  mutate(viagens_obj = ceiling(viagens)) %>%
  mutate(end_time = if_else(row_number() == n() & inteiro==F,start_time+(intervalo_pratico*viagens_obj),end_time)) %>%
  mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
         inteiro = (viagens - as.integer(viagens)),
         inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
  ungroup()

freq_ida <- rbindlist(list(freq_ida_manha,freq_ida_tarde), fill = T)
freq_nao_espelhado <- rbindlist(list(freq_ida,freq_volta), fill=T)

## Ambos ----

freq_ambos <- freq_bruta_proc %>%
  filter(modelo_espelhado == T)

if(nrow(freq_ambos) > 0){
  
  for (z in 1:5){
    freq_ambos <- freq_ambos %>%
      group_by(servico,sentido) %>%
      mutate(end_time = if_else(intervalo_pratico > difftime(end_time,start_time, units = "secs") &
                                  start_time < horario_inicio+3600,start_time+intervalo_pratico,end_time)) %>%
      mutate(start_time = if_else(row_number() != 1,lag(end_time),start_time)) %>%
      filter(start_time < horario_fim) %>%
      ungroup()
  }
  
  # freq_ambos <- freq_ambos %>%
  #   select(-c(teste))
  
  while (length(unique(freq_ambos$teste))!=1){
    freq_ambos <- freq_ambos %>%
      group_by(servico,sentido) %>%
      mutate(end_time = if_else(lead(intervalo_pratico)==intervalo_pratico & !is.na(lead(intervalo_pratico)),lead(end_time),end_time)) %>%
      filter(end_time != lag(end_time) | is.na(lag(end_time))) %>%
      mutate(teste = intervalo_pratico==lead(intervalo_pratico)) %>%
      mutate(teste = if_else(is.na(teste),FALSE,teste))
  }
  
  freq_ambos <- freq_ambos %>%
    ungroup() %>%
    select(-teste) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE),
           minutos = as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "mins")))  %>%
    mutate(end_time_original = end_time) %>%
    select(servico,sentido,start_time,end_time,end_time_original,frota_hora,intervalo_pratico_min,perc_pax,everything())
  
  
  while (all(freq_ambos$inteiro)!=TRUE){
    freq_ambos <- freq_ambos %>%
      group_by(servico,sentido) %>%
      mutate(end_time = if_else(
        inteiro,end_time,
        if_else(lead(intervalo_pratico)<intervalo_pratico | is.na(lead(intervalo_pratico)),
                start_time+as.integer(minutos/(intervalo_pratico/60))*intervalo_pratico,
                start_time+as.integer(minutos/(intervalo_pratico/60)+1)*intervalo_pratico))) %>%
      mutate(end_time = as.POSIXct(end_time, origin='1970-01-01')) %>%
      ungroup() %>%
      arrange(servico,sentido,start_time)
    
    freq_ambos <- freq_ambos %>%
      group_by(servico,sentido) %>%
      filter(row_number() == 1 | row_number() == n() | difftime(end_time,start_time,units = "secs")>intervalo_pratico) %>%
      mutate(start_time=if_else(!is.na(lag(end_time)),lag(end_time),start_time)) %>%
      mutate(start_time = as.POSIXct(start_time, origin='1970-01-01')) %>%
      filter(start_time < horario_fim) %>%
      ungroup() %>%
      arrange(servico,sentido,start_time)
    
    freq_ambos <- freq_ambos %>%
      group_by(servico,sentido) %>%
      mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
             inteiro = (viagens - as.integer(viagens)),
             inteiro = if_else(inteiro==0,TRUE,FALSE),
             minutos = as.numeric(difftime(as.POSIXct(end_time_original), as.POSIXct(start_time), units = "mins"))) %>%
      filter(start_time != end_time) %>%
      ungroup() %>%
      arrange(servico,sentido,start_time)
  }
  
  freq_ambos <- freq_ambos %>%
    group_by(servico,sentido) %>%
    arrange(end_time) %>%
    mutate(end_time = if_else(row_number() == n() & end_time < horario_fim,horario_fim,end_time)) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
    mutate(viagens_obj = ceiling(viagens)) %>%
    mutate(end_time = if_else(row_number() == n() & inteiro==F,start_time+(intervalo_pratico*viagens_obj),end_time)) %>%
    mutate(viagens = as.numeric(as.numeric(difftime(as.POSIXct(end_time), as.POSIXct(start_time), units = "secs")) / intervalo_pratico),
           inteiro = (viagens - as.integer(viagens)),
           inteiro = if_else(inteiro==0,TRUE,FALSE)) %>%
    ungroup()
  
}

freq_final <- rbindlist(list(freq_nao_espelhado,freq_ambos), fill=T) %>%
  arrange(servico,sentido,start_time) %>%
  select(servico,sentido,start_time,end_time,viagens,intervalo_pratico_min,intervalo_teorico,frota_referencia,frota_hora,intervalo_pratico) %>%
  rename(frota_linha = frota_referencia,
         intervalo_pico = intervalo_teorico,
         headway_secs = intervalo_pratico,
         intervalo = intervalo_pratico_min,
         frota = frota_hora)

sumario <- freq_final %>%
  group_by(servico) %>%
  summarise(inicio_operacao = min(start_time),
            fim_operacao = max(end_time),
            intervalo_min = min(intervalo),
            intervalo_max = max(intervalo),
            viagens_dia = sum(viagens),
            frota_operante = max(frota_linha)) %>%
  mutate(inicio_operacao = as.character(as.ITime(inicio_operacao)),
         fim_operacao = as.character(as.ITime(fim_operacao))) %>%
  left_join(select(linhas,servico,viagem_inicial)) %>%
  mutate(viagens_dia = if_else(viagem_inicial=='circular',viagens_dia,viagens_dia/2)) %>%
  ungroup() %>% 
  mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
         numero = stringr::str_extract(servico, "[0-9]+" )) %>% 
  tidyr::unite(.,servico,letras,numero, na.rm = T,sep='') %>% 
  select(servico,everything()) %>% 
  mutate(inicio_operacao = if_else(lubridate::hour(hms(inicio_operacao)) < 2,
                              hms(inicio_operacao)+lubridate::hours(24),
                              hms(inicio_operacao))) %>% 
  mutate(fim_operacao = if_else(lubridate::hour(hms(fim_operacao)) < head(lubridate::hour(hms(inicio_operacao))),
                            hms(fim_operacao)+lubridate::hours(24),
                            hms(fim_operacao))) %>% 
  mutate(fim_operacao = if_else(inicio_operacao > fim_operacao,
                                hms(fim_operacao)+lubridate::hours(24),
                                hms(fim_operacao))) %>% 
  mutate(inicio_operacao = paste(sprintf("%02d", lubridate::hour(inicio_operacao)),
                            sprintf("%02d", lubridate::minute(inicio_operacao)),
                            sprintf("%02d", lubridate::second(inicio_operacao)),sep=':'),
         fim_operacao = paste(sprintf("%02d", lubridate::hour(fim_operacao)),
                          sprintf("%02d", lubridate::minute(fim_operacao)),
                          sprintf("%02d", lubridate::second(fim_operacao)),sep=':'))



freq_final <- freq_final %>%
  group_by(servico,sentido) %>%
  arrange(servico,sentido,start_time) %>%
  mutate(time_gap = ifelse(row_number() == 1,0,start_time-lag(end_time)))

partidas <- freq_final %>% group_by(servico,sentido) %>% summarise(partidas = sum(viagens))

erros <- length(which(freq_final$time_gap != 0 | is.na(freq_final$time_gap)))

if (erros > 0){
  print('Erro! Rever cálculos.')
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
  beepr::beep()
}

pasta_ano <- paste0("./resultados/sumario/",ano_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_ano)), dir.create(file.path(getwd(),pasta_ano)), FALSE)
pasta_mes <- paste0("./resultados/sumario/",ano_gtfs,"/",mes_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_mes)), dir.create(file.path(getwd(),pasta_mes)), FALSE)
end_sumario <- paste0(pasta_mes,"/sumario_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q_",tipo_dia,".csv")

if(file.exists(end_sumario)){
  sumario_original <- fread(end_sumario) %>% 
    mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
           numero = stringr::str_extract(servico, "[0-9]+" )) %>% 
    tidyr::unite(.,servico,letras,numero, na.rm = T,sep='') %>% 
    select(servico,everything()) %>% 
    filter(servico %nin% sumario$servico) %>% 
    mutate(intervalo_min = as.numeric(sub(",", ".", intervalo_min, fixed = TRUE)),
           intervalo_max = as.numeric(sub(",", ".", intervalo_max, fixed = TRUE)),
           viagens_dia = as.numeric(sub(",", ".", viagens_dia, fixed = TRUE)))
  sumario_combi <- sumario %>% 
    bind_rows(sumario_original) %>% 
    select(1:8) %>% 
    arrange(servico)
} else {
  sumario_combi <- sumario
}


fwrite(sumario_combi,end_sumario, sep=";",dec=",")

freq_final <- freq_final %>%
  mutate(start_time = as.character(as.ITime(start_time)),
         end_time = as.character(as.ITime(end_time))) %>%
  select(servico,sentido,start_time,end_time,viagens,intervalo) %>% 
  rename(partidas = viagens) %>% 
  group_by(servico,sentido) %>% 
  mutate(start_time = if_else(lubridate::hour(lubridate::hms(start_time)) < 2,
                              hms(start_time)+lubridate::hours(24),
                              hms(start_time))) %>% 
  mutate(start_time = if_else(lubridate::hour(hms(start_time)) < head(lubridate::hour(hms(start_time))),
                              hms(start_time)+lubridate::hours(24),
                              hms(start_time))) %>% 
  mutate(end_time = if_else(lubridate::hour(hms(end_time)) < head(lubridate::hour(hms(start_time))),
                            hms(end_time)+lubridate::hours(24),
                            hms(end_time))) %>% 
  mutate(start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
                            sprintf("%02d", lubridate::minute(start_time)),
                            sprintf("%02d", lubridate::second(start_time)),sep=':'),
         end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
                          sprintf("%02d", lubridate::minute(end_time)),
                          sprintf("%02d", lubridate::second(end_time)),sep=':')) %>% 
  mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
         numero = stringr::str_extract(servico, "[0-9]+" )) %>% 
  tidyr::unite(.,servico,letras,numero, na.rm = T,sep='') %>% 
  select(servico,everything())

pasta_ano <- paste0("./resultados/quadro_horario/",ano_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_ano)), dir.create(file.path(getwd(),pasta_ano)), FALSE)
pasta_mes <- paste0("./resultados/quadro_horario/",ano_gtfs,"/",mes_gtfs)
ifelse(!dir.exists(file.path(getwd(),pasta_mes)), dir.create(file.path(getwd(),pasta_mes)), FALSE)
end_qh <- paste0(pasta_mes,"/qh_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q_",tipo_dia,".csv")


if(file.exists(end_qh)){
  qh_original <- fread(end_qh) %>%
    select(1:6) %>%
    mutate(letras = stringr::str_extract(servico, "[A-Z]+" ),
           numero = stringr::str_extract(servico, "[0-9]+" )) %>% 
    tidyr::unite(.,servico,letras,numero, na.rm = T,sep='') %>% 
    select(servico,everything()) %>% 
    filter(servico %nin% sumario$servico) %>%
    mutate(intervalo = as.numeric(sub(",", ".", intervalo, fixed = TRUE))) %>% 
    mutate(partidas = as.numeric(sub(",", ".", partidas, fixed = TRUE)))
  qh_combi <- freq_final %>%
    bind_rows(qh_original) %>%
    select(1:6) %>%
    arrange(servico)
} else {
  qh_combi <- freq_final
}

fwrite(qh_combi,end_qh, sep=";",dec=",")