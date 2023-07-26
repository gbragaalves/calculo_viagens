library(data.table)
library(dplyr)
library(Hmisc)
library(lubridate)

ano <- '2023'
mes <- '06'

dicionario_lecd <- fread('../../dados/insumos/correspondencia_servico_lecd.csv')

data_inicio <- as.Date(paste0(ano,'-',mes,'-01'))

data_fim <- data_inicio + months(1) - days(1)

basedosdados::set_billing_id("rj-smtr")

query_rho <- glue::glue("SELECT linha, data_transacao, hora_transacao, total_gratuidades, total_pagantes_especie, total_pagantes_cartao, data_processamento  FROM `rj-smtr.br_rj_riodejaneiro_rdo.rho5_registros_sppo` WHERE data_transacao BETWEEN '{data_inicio}' and '{data_fim}'")
  
rho <- basedosdados::read_sql(query_rho)
 
rho <- rho %>%
  mutate(linha = trimws(linha),
         linha = gsub('A1','',linha),
         linha = gsub('V1','SV',linha),
         linha = gsub('B1','SP',linha),
         linha = gsub('VT','',linha),
         linha = gsub('SN','',linha),
         linha = gsub('N1','',linha),
         linha = gsub('TRO1','100',linha),
         linha = gsub('TRO6','108',linha),
         linha = gsub('LECD28A','410',linha),
         linha = gsub('LECD37V','LECD37',linha),
         linha = gsub('SV624','LECD36',linha),
         linha = gsub('LECD36','LECD36',linha),
         linha = gsub('LECD37','LECD37',linha),
         linha = gsub('LECD38','LECD38',linha),
         linha = gsub('LECD39','LECD39',linha),
         linha = gsub('LECD40','LECD40',linha),
         linha = gsub('LECD41','LECD41',linha),
         linha = gsub('LECD 41V','LECD41',linha),
         linha = gsub('957V','LECD41',linha),
         linha = gsub('LECD42','LECD42',linha),
         linha = gsub('LECD43','LECD43',linha),
         linha = gsub('LECD44','LECD44',linha),
         linha = gsub('LECD45','LECD45',linha),
         linha = gsub('LECD46','LECD46',linha),
         linha = gsub('LECD47','LECD47',linha),
         linha = gsub('LECD48','LECD48',linha),
         linha = gsub('LECD49','LECD49',linha),
         linha = gsub('LECD50','LECD50',linha),
         linha = gsub('LECD51','LECD51',linha),
         linha = gsub('443','LECD37',linha),
         linha = gsub('645','LECD38',linha),
         linha = gsub('817','LECD39',linha),
         linha = gsub('881','LECD40',linha),
         linha = gsub('957','LECD41',linha),
         linha = gsub('990','LECD42',linha),
         linha = gsub('987','LECD43',linha),
         linha = gsub('808','LECD44',linha),
         linha = gsub('899','LECD45',linha),
         linha = gsub('874','LECD45',linha),
         linha = gsub('157','LECD49',linha),
         linha = gsub('517','LECD49',linha),
         linha = gsub('605','LECD50',linha),
         linha = gsub('2310 R','2310',linha),
         linha = gsub('2333R','2333',linha),
         linha = gsub('2333R','2333',linha),
         linha = gsub('2345C','2345',linha),
         linha = gsub('2345K','2345',linha),
         linha = gsub('2333SV','SV2333',linha),
         linha = gsub('362X','362',linha),
         linha = gsub('426A','426',linha),
         linha = gsub('624SV','LECD36',linha),
         linha = gsub('LECD28','410',linha),
         linha = gsub('SVB665V','SVB665',linha),
         linha = gsub('SVA665V','SVA665',linha),
         linha = gsub('INT9','554',linha),
         linha = gsub('2802R','2802',linha),
         linha = gsub('513SP','513',linha),
         linha = gsub('265SP','SP265',linha),
         linha = gsub('328 AR','328',linha),
         linha = gsub('433PB','433',linha),
         linha = gsub('474SV','SV474',linha),
         linha = gsub('774SV','SV774',linha),
         linha = gsub('790SV','SV790',linha),
         linha = gsub('810SP','SP810',linha),
         linha = gsub('838SP','SP838',linha),
         linha = gsub('841SP','LECD44',linha),
         linha = gsub('843SV','SV843',linha),
         linha = gsub('917SV','SV917',linha),
         linha = gsub('LECD41V','LECD41',linha),
         linha = gsub('LECD49V','LECD49',linha),
         linha = gsub('206','006',linha),
         linha = trimws(linha),
         data = as.Date(data_transacao),
         hora = as.ITime(paste0(hora_transacao,":00:00"))) %>%
  filter(substr(linha,1,2) %nin% c('TO','TL','TC')) %>% 
  left_join(dicionario_lecd, by = c('linha' = 'LECD')) %>% 
  mutate(linha = if_else(!is.na(servico),servico,linha)) %>% 
  select(-c(servico)) %>% 
  rename(servico = linha)

rho_cons <- rho %>%  
  group_by(servico,data,hora) %>%
  slice(which.max(as.Date(data_processamento))) %>%
  mutate_at(vars(4:6),as.numeric) %>% 
  summarise(gratuidade = sum(total_gratuidades),
            dinheiro = sum(total_pagantes_especie),
            cartao = sum(total_pagantes_cartao)) %>%
  mutate(demanda = gratuidade + dinheiro + cartao) %>% 
  filter(lubridate::year(data) == as.integer(ano),
         as.integer(lubridate::month(data)) == as.integer(mes))

pasta_ano <- paste0("../../dados/demanda/",ano)
ifelse(!dir.exists(file.path(getwd(),pasta_ano)), dir.create(file.path(getwd(),pasta_ano)), FALSE)

saveRDS(rho_cons,paste0(pasta_ano,"/rho_",ano,"_",mes,".rds"))

