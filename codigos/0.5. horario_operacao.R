library(bizdays)
library(lubridate)
library(dplyr)
library(data.table)

ano <- '2023'
mes <- '05'

dicionario_lecd <- fread('../../dados/insumos/correspondencia_servico_lecd.csv')

end_pasta <- paste0("../../dados/viagens/sppo/",ano,"/",mes)

nm <- list.files(path=end_pasta, full.names = T,pattern = "\\.rds$")

trips <- do.call(rbind, lapply(nm, function(x) readRDS(file=x))) %>%
  left_join(dicionario_lecd, by = c('servico_realizado' = 'LECD')) %>% 
  mutate(servico_realizado = if_else(!is.na(servico),servico,servico_realizado)) %>% 
  select(-c(servico))

load_calendar("../../dados/calendario.json")

calculate_operacao <- function(viagens, dias) {
  inicio <- viagens %>%
    mutate(hora_partida = as.ITime(datetime_partida - 9000),
           hora_partida = hora_partida + 9000) %>%
    arrange(hora_partida) %>%
    group_by(servico_realizado) %>%
    slice_head(n = dias) %>%
    summarise(inicio = mean(hora_partida)) %>%
    mutate(inicio = as.POSIXct(inicio),
           inicio = round_date(inicio, unit = "15 minutes"),
           inicio = as.ITime(inicio)) 
  
  fim <- viagens %>%
    mutate(hora_partida = as.ITime(datetime_partida - 9000),
           hora_partida = hora_partida + 9000) %>%
    arrange(hora_partida) %>%
    group_by(servico_realizado) %>%
    slice_tail(n = dias) %>%
    summarise(fim = mean(hora_partida)) %>%
    mutate(fim = as.POSIXct(fim),
           fim = round_date(fim, unit = "15 minutes"),
           fim = as.ITime(fim))
  
  operacao <- inicio %>% 
    left_join(fim)
  
  return(operacao)
}

# Filter trips for business days in Rio Janeiro
trips_du <- trips %>%
  filter(is.bizday(data, "Rio_Janeiro"))

# Calculate operation times for business days
dias_uteis <- length(unique(trips_du$data))
operacao_du <- calculate_operacao(trips_du, dias_uteis)

# Filter trips for Saturdays
trips_sabado <- trips %>%
  filter(wday(as.Date(data)) %in% 7)

# Calculate operation times for Saturdays
sabados <- length(unique(trips_sabado$data))
operacao_sabado <- calculate_operacao(trips_sabado, sabados)

# Filter trips for Sundays
trips_domingo <- trips %>%
  filter(wday(as.Date(data)) %in% 1)

# Calculate operation times for Sundays
domingos <- length(unique(trips_domingo$data))
operacao_domingo <- calculate_operacao(trips_domingo, domingos)

names(operacao_du) <- paste0(names(operacao_du),"_du") 
names(operacao_sabado) <- paste0(names(operacao_sabado),"_sab") 
names(operacao_domingo) <- paste0(names(operacao_domingo),"_dom")

# Combine the operation times
operacao <- operacao_du %>%
  rename(servico = servico_realizado_du) %>% 
  left_join(operacao_sabado, by = c("servico"="servico_realizado_sab")) %>%
  left_join(operacao_domingo, by = c("servico"="servico_realizado_dom"))

fwrite(operacao, paste0("../../dados/viagens/sppo/",ano,"/",mes,"/horario_operacao_",ano,"_",mes,".csv"))

