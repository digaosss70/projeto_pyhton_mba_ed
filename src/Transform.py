#importação de pacotes
import pandas as pd
from datetime import datetime

#função para transformar horas de vooo em minutos de voo
def Tempo_voo_horas(tempo_voo_horas):
    return tempo_voo_horas*60

#função para adicionar turno de voo com base em hora
def Turno_partida(hora):
    if (hora >= 0 and hora < 6):
        return 'MADRUGADA'
    elif (hora >= 6 and hora < 12):
        return 'MANHA' 
    elif (hora >= 12 and hora < 18):
        return 'TARDE'
    elif (hora >= 18 and hora <= 23):
        return 'NOITE'

#função que ira tratar nosso csv
def transform(csv_source,csv_target):
    df = pd.read_csv(
    f"data/bronze/{csv_source}.csv",
    index_col=0
    )

    #adicionando novas colunas
    df["tempo_voo_minutos"] = df.loc[:,"tempo_voo"].apply(lambda x: Tempo_voo_horas(x))
    df["Turno_partida"] = df.loc[:,"hora"].apply(lambda x: Turno_partida(x))

    #print(df.columns)
    df.to_csv(f"data/silver/{csv_target}.csv")

    print(df.head())


