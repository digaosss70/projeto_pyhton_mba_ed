
#importando pacotes que serão utilizados durante a limpeza
import pandas as pd
import datetime
import numpy as np
import re
import logging

#Função para limpeza de strings
    #Apenas caracteres de a até z, A até Z e 0 até 9
    #Transforma o string em maiusculas
def padroniza_str(obs):
    return re.sub('[^A-Za-z0-9]+', '', obs.upper())

#Função principal que irá realizar todas as lipezas
def dataClean(csv_raw,csv_bronze_target):
    #Cria arquivo de log
    logging.basicConfig(filename='flights_pipe.log', level=logging.INFO) #vai criar um arquivo para os logs
    logger = logging.getLogger()

    #concatena uma linha no arquivo de log com a data/hora de agora
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')

    #Leitura do csv fonte
    df = pd.read_csv(
    csv_raw,
    index_col=0
    )

    df.to_csv(f"data/raw/{csv_bronze_target}.csv", index=False)

    #lista que será utilizada para filtrar apenas colunas que serão utilizadas
    usecols=["year", "month",  "day", "hour", "minute","arr_delay","carrier","flight","air_time","distance", "origin", "dest"]

    #Iremos verificar se as colunas contidas na lista usecols existem no datafram df
        #caso existam colunas na lista usecols que não existem no dataframe df iremos gerar uma linha no arquivo de log e gera uma exceção
    if not set(usecols).issubset(set(df.columns)):
        logger.error(f"Mudança de schema; {datetime.datetime.now()} ")
        raise Exception("Mudança de schema")

    #dataframe df_raw filtrando registros
    df_raw = df.loc[
        (~df["carrier"].isna()) \
        & (~df["flight"].isna()) \
        & (~df["year"].isna()) \
        & (~df["hour"].isna()) \
        & (~df["minute"].isna()) \
        & (~df["month"].isna()) \
        & (~df["day"].isna()) \
        & (df["air_time"] > 0)
    ].loc[:, usecols]

    #Iremos verificar se fitlragem de registros excluiu mais de 5% de registros
        #caso positivo iremos gerar uma linha no arquivo de log
    if 1 - len(df_raw)/len(df) > 0.05:
        logger.warning(f"Muitas observações perdidas na seleção; {datetime.datetime.now()} ")

    #removendo duplicadas do dataframe df_raw e salvando no mesmo dataframe
    df_raw.drop_duplicates(inplace=True)   

    #criando campo date_time com base nos campos ("year", "month", "day", "hour", "minute")
    df_raw["date_time"] =  pd.to_datetime(df_raw[["year", "month", "day", "hour", "minute"]],  dayfirst=True)

    #lista com nome de campos atual
    old_columns =["date_time","hour","arr_delay","carrier","flight","air_time","distance", "origin", "dest" ] 

    #lista com novos nomes de campos
    new_columns = ["data_hora","hora","atraso_chegada", "companhia", "id_voo","tempo_voo", "distancia", "origem", "destino"]

    #dicionario que irá ser usado para renomear as colunas
    columns_map = {old_columns[i]: new_columns[i] for i in range(len(old_columns))}

    #novo datafram df_work que copia o dataframe df_raw apenas com as colunas selecionadas
    df_work = df_raw.loc[:, old_columns].copy()

    #renomeando colunas do dataframe df_work
    df_work.rename(columns=columns_map, inplace=True)

    #alterando os datatypes das colunas
    df_work["distancia"] = df_work.loc[:,"distancia"].astype(float)
    df_work["hora"] = df_work.loc[:,"hora"].astype(int)
    df_work["companhia"] = df_work.loc[:,"companhia"].astype(str)
    df_work["id_voo"] = df_work.loc[:,"id_voo"].astype(str)
    df_work["atraso_chegada"] = df_work.loc[:,"atraso_chegada"].astype(str)
    df_work["origem"] = df_work.loc[:,"origem"].astype(str)
    df_work["destino"] = df_work.loc[:,"destino"].astype(str)  

    #ajustando as colunas de string (removendo caracteres especiais e transformando em maiusculas)
    df_work["companhia"] = df_work.loc[:,"companhia"].apply(lambda x: padroniza_str(x))
    df_work["id_voo"] = df_work.loc[:,"id_voo"].apply(lambda x: padroniza_str(x))
    df_work["origem"] = df_work.loc[:,"origem"].apply(lambda x: padroniza_str(x))
    df_work["destino"] = df_work.loc[:,"destino"].apply(lambda x: padroniza_str(x))

    #exporta trantamentos para um novo csv
    df_work.to_csv(f"data/bronze/{csv_bronze_target}.csv", index=False)

    #concatena uma linha no arquivo de log com a data/hora de agora
    logger.info(f'fim da execução ; {datetime.datetime.now()}')          