#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import time
import re
import pandas_gbq as pdg

from google.cloud import bigquery
from google.cloud import storage
import google.auth

credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=your_project_id)


def get_data():
    
    """
    Função para extrair os dados das respostas da pesquisa

    Parameters
    ----------
    model_name : str
        Nome do modelo.
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_respostas = f"""

    SELECT 
    dataref,
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=1 THEN DATE_ADD(dataref, INTERVAL -6 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=2 THEN DATE_ADD(dataref, INTERVAL -0 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=3 THEN DATE_ADD(dataref, INTERVAL -1 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=4 THEN DATE_ADD(dataref, INTERVAL -2 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=5 THEN DATE_ADD(dataref, INTERVAL -3 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=6 THEN DATE_ADD(dataref, INTERVAL -4 DAY)
      WHEN EXTRACT(DAYOFWEEK FROM dataref)=7 THEN DATE_ADD(dataref, INTERVAL -5 DAY)
      END monday,
    EXTRACT(YEAR FROM dataref) AS year,
    EXTRACT(ISOWEEK FROM dataref) AS isoweek,
    Classificacao,
    CASE WHEN dataref<'2024-02-09' THEN 5.0 ELSE 7.5 END frac_sorteados
    FROM `mydataset.NPS.nps_answers` 
    WHERE TRUE
      AND dataref>='2023-11-01'  -- uniformidade da data do gatilho para D-1
    ORDER BY dataref

    """    
  
    # Execute a query
    df_ = client.query(query_respostas).to_dataframe()
            
    return df_


def correct_dtypes(df):
    
    """
    Função para corrigir os dtypes
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    df_ = df.copy()
    df_['dataref'] = pd.to_datetime(df_['dataref']).dt.strftime('%Y-%m-%d')
    df_['monday'] = pd.to_datetime(df_['monday']).dt.strftime('%Y-%m-%d')
    
    return df_


def save_data(df, destination_table, destination_file_name, folder, savedate):
    
    pdg.to_gbq(df, destination_table, project_id=your_project_id, credentials=credentials, if_exists='replace') 
    
    df.to_parquet(folder + f'silver/{destination_file_name}_{savedate}.parquet')


def main(folder, model_name):
    """
    Função principal para executar os processos de get_data.
    """
    # Fixar savedate para todos os modulos
    savedate = str(dt.date.today())[:10]
    
    folder = folder + f'{model_name}_prediction/'   
    destination_file_name = f'{model_name}_data' 
    destination_table = 'mydataset.MYTABLE_TEMP.' + destination_file_name
    
    print('Iniciando o processo get_data')
    ini = time.time()
    df_ = get_data()
    fim = time.time()
    print(f'Finalizado o processo get_data em {(fim-ini):.2f} segundos')
    
    print('Iniciando o processo correct_dtypes')
    ini = time.time()
    df = correct_dtypes(df_)
    fim = time.time()
    print(f'Finalizado o processo correct_dtypes em {(fim-ini):.2f} segundos')

    print('Iniciando o processo save_data')
    ini = time.time()
    save_data(df, destination_table, destination_file_name, folder, savedate)
    fim = time.time()
    print(f'Finalizado o processo save_data em {(fim-ini):.2f} segundos')
    print()
    print(folder+destination_file_name)
    print(destination_table)

folder = 'gs://myproject/Projeto_NPS/production/'
model_name = 'nps'

main(folder, model_name)

print('end of job')

