##importa dos .py as funções pricipais
from DataClean import dataClean
from Transform import transform

##definição de ambiente (neste caso arquivos de raw,bronze, silver)
csv_raw = 'https://raw.githubusercontent.com/JackyP/testing/master/datasets/nycflights.csv'
csv_bronze = 'csv_voos'
csv_silver = 'csv_voos_tratado'

#realiza limpeza (etapa bronze)
dataClean(\
    csv_raw,\
    csv_bronze\
    )

##realiza tratamento (etapa silver)
transform(\
    csv_bronze,\
    csv_silver\
    )


