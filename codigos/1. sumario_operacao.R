library(bizdays)
library(lubridate)
library(dplyr)
library(data.table)

ano <- '2023'
mes <- '05'
tipo_dia <- 'du'
dia_num <- case_when(tipo_dia == 'sab' ~ 7,
                     tipo_dia == 'dom' ~ 1,
                     tipo_dia == 'du' ~ NA_real_)

dicionario_lecd <- fread('../../dados/insumos/correspondencia_servico_lecd.csv')

end_pasta <- paste0("../../dados/viagens/sppo/",ano,"/",mes)

nm <- list.files(path=end_pasta, full.names = T, pattern = "*.rds")

trips <- do.call(rbind, lapply(nm, function(x) readRDS(file=x)))

load_calendar("../../dados/calendario.json")

trips <- trips %>%
  mutate(tipo_dia = case_when(tipo_dia == 'Dia Útil' ~ 'du',
                              tipo_dia == 'Sabado' ~ 'sab',
                              tipo_dia == 'Domingo' ~ 'dom',
                              TRUE ~ tipo_dia)) %>% 
  {if (tipo_dia=="du") filter(., is.bizday(data,"Rio_Janeiro")) else filter(., lubridate::wday(data) == dia_num)} %>% 
  arrange(servico_realizado,sentido,datetime_partida) %>%
  mutate(hora_partida = lubridate::hour(datetime_partida)) %>%
  group_by(servico_informado,sentido,data) %>%
  mutate(intervalo = difftime(datetime_partida,lag(datetime_partida),units = "secs"),
         tempo_viagem_ajustado = case_when(servico_informado == '851' & sentido == 'I' ~ as.integer(difftime(as.POSIXct(datetime_chegada),as.POSIXct(datetime_partida),units='secs'))+1800,
                                           servico_informado == 'SP485' & sentido == 'I' ~ as.integer(difftime(as.POSIXct(datetime_chegada),as.POSIXct(datetime_partida),units='secs'))+1200,
                                           TRUE ~ as.integer(difftime(as.POSIXct(datetime_chegada),as.POSIXct(datetime_partida),units='secs'))+360),
         velocidade_media = round((distancia_planejada*1000)/(tempo_viagem_ajustado)*3.6,2)) %>%
  mutate(intervalo = case_when(as.numeric(intervalo) <= 180 ~ plyr::round_any(as.numeric(intervalo), 30),
                               as.numeric(intervalo) > 180 ~ plyr::round_any(as.numeric(intervalo), 60),
                               is.na(intervalo) ~ as.numeric(intervalo))) %>%
  ungroup() %>%
  group_by(servico_realizado,sentido) %>%
  filter(!tempo_viagem_ajustado %in% boxplot.stats(tempo_viagem_ajustado)$out) %>%
  ungroup() %>%
  left_join(dicionario_lecd, by = c('servico_realizado' = 'LECD')) %>% 
  mutate(servico_realizado = if_else(!is.na(servico),servico,servico_realizado)) %>% 
  select(-c(servico))

dias_considerar <- length(unique(trips$data))

inicio_operacao <- trips %>%
  mutate(hora_partida = as.ITime(datetime_partida-9000)) %>%
  arrange(hora_partida) %>%
  mutate(hora_partida = hora_partida+9000) %>%
  dplyr::group_by(servico_informado) %>%
  slice_head(n=dias_considerar) %>%
  dplyr::summarise(inicio = mean(hora_partida)) %>%
  mutate(inicio = as.POSIXct(inicio),
         inicio = round_date(inicio,unit="15 minutes"),
         inicio = as.ITime(inicio))

fim_operacao <- trips %>%
  mutate(hora_partida = as.ITime(datetime_partida-9000)) %>%
  arrange(hora_partida) %>%
  mutate(hora_partida = hora_partida+9000) %>%
  dplyr::group_by(servico_informado) %>%
  slice_tail(n=dias_considerar) %>%
  dplyr::summarise(fim = mean(hora_partida)) %>%
  mutate(fim = as.POSIXct(fim),
         fim = round_date(fim,unit="15 minutes"),
         fim = as.ITime(fim))

operacao <- inicio_operacao %>%
  left_join(fim_operacao) %>%
  rename(servico = servico_informado)

rm(inicio_operacao,fim_operacao)

linhas_circulares <- trips %>% 
  filter(sentido == 'C') %>% 
  select(servico_realizado) %>% 
  distinct() %>% 
  unlist()

media_viagens <- trips %>%
  group_by(servico_informado,data) %>%
  count() %>%
  ungroup() %>%
  group_by(servico_informado) %>%
  summarise(media_viagens = round(mean(n)),
            terceiro_quartil = round(quantile(n,0.75))) %>% 
  rename(servico = servico_informado) %>% 
  mutate(media_viagens = if_else(servico %in% linhas_circulares,media_viagens,media_viagens/2),
         terceiro_quartil = if_else(servico %in% linhas_circulares,terceiro_quartil,terceiro_quartil/2))

trips <- trips %>%
  filter(!is.na(intervalo),
         intervalo > 0,
         intervalo < 10800,
         velocidade_media > 5,
         velocidade_media < 80,
  ) %>%
  ungroup()

sumario <- trips %>%
  dplyr::group_by(servico_realizado,sentido,hora_partida) %>%
  dplyr::summarise(intervalo = mean(intervalo),
                   tempo_viagem = mean(tempo_viagem_ajustado),
                   velocidade_media = round(mean(velocidade_media),3)) %>%
  mutate(intervalo = case_when(as.numeric(intervalo) <= 180 ~ plyr::round_any(as.numeric(intervalo), 30),
                               as.numeric(intervalo) > 180 ~ plyr::round_any(as.numeric(intervalo), 60))) %>%
  ungroup() %>%
  mutate(tempo_viagem = round(tempo_viagem,0))

pasta_ano <- paste0("../../dados/sumarios/",ano)
ifelse(!dir.exists(file.path(getwd(),pasta_ano)), dir.create(file.path(getwd(),pasta_ano), recursive = TRUE), FALSE)
end_sumario <- paste0(pasta_ano,"/sumario_sent_hora_",mes,"_", tipo_dia, ".csv")

fwrite(sumario,end_sumario)

intervalos_linhas <- sumario %>%
  ungroup() %>% 
  group_by(servico_realizado) %>%
  slice_min(intervalo,n=3, with_ties = F) %>%
  summarise(pico = mean(intervalo)) %>%
  mutate(pico = case_when(as.numeric(pico) <= 360 ~ plyr::round_any(as.numeric(pico), 30),
                          as.numeric(pico) > 360 ~ plyr::round_any(as.numeric(pico), 60))) %>%
  mutate(intervalo_legivel = pico/60) %>% 
  rename(servico = servico_realizado)

sumario_operacao <- operacao %>% 
  left_join(media_viagens) %>% 
  left_join(intervalos_linhas)

end_sumario <- paste0(pasta_ano,"/sumario_operacao_",mes,"_", tipo_dia, ".csv")

fwrite(sumario_operacao,end_sumario,sep=';',dec=',')

