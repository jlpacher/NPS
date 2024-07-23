#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas            as pd
import numpy             as np
import datetime as dt
import time
import pandas_gbq as pdg


from google.cloud import bigquery
from google.cloud import bigquery_storage
import google.auth

credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=your_project_id)


def save_history_bq():
    
    """
    Função para adicionar dados às tabelas existentes ou criar se não existentes (só na primeira execução).
    Só guarda uma execução por dia (a última).
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_to_bq = f"""
    
    --corrigir dtypes
    CREATE OR REPLACE TABLE `mydataset.MYTABLE_TEMP.nps_forecast_semana_corrigido` AS (
    SELECT
    id,
    DATE(monday) AS monday,	
    year,
    isoweek,
    dias,
    DATE(data_atualizacao) AS data_atualizacao,
    CAST(promotor AS INTEGER) AS promotor,
    CAST(neutro AS INTEGER) AS neutro,
    CAST(detrator AS INTEGER) AS detrator,
    CAST(respondentes AS INTEGER) AS respondentes,
    frac_sorteados,
    respondentes_ajustado,
    nps,
    forecast_value,
    confidence_interval_lower_bound,
    confidence_interval_upper_bound,
    respondentes_fcst
    FROM `mydataset.MYTABLE_TEMP.nps_forecast_semana`
    )
    ;
    CREATE OR REPLACE TABLE `mydataset.MYTABLE_TEMP.nps_forecast_mes_corrigido` AS (
    SELECT
    DATE(data_atualizacao) AS data_atualizacao,
    DATE(mes) AS mes,
    nps_real,
    nps_fcst
    FROM `mydataset.MYTABLE_TEMPP.nps_forecast_mes`
    )
    ;

    -- salvar predicoes no bq
    -- primeira execucao somente (demais, deixar comentado)
    CREATE OR REPLACE TABLE `mydataset.MYTABLE_TEMP.NPS_PREDICAO` AS (
      SELECT * FROM `mydataset.MYTABLE_TEMP.nps_forecast_semana_corrigido`
    ORDER BY data_atualizacao, id)
    ;
    CREATE OR REPLACE TABLE `mydataset.MYTABLE_TEMP.NPS_MENSAL_DASHBOARD` AS (
      SELECT * FROM `mydataset.MYTABLE_TEMP.nps_forecast_mes_corrigido`
    ORDER BY 1,2)
    ;
    -- -- demais execucoes: apaga dados somente da mesma data_atualizaco
    -- DELETE FROM `mydataset.MYTABLE_TEMP.NPS_PREDICAO` WHERE data_atualizacao IN (
    --   SELECT data_atualizacao FROM `mydataset.MYTABLE_TEMP.nps_forecast_semana_corrigido`)
    -- ;
    -- DELETE FROM `mydataset.MYTABLE_TEMP.NPS_MENSAL_DASHBOARD` WHERE data_atualizacao IN (
    --   SELECT data_atualizacao FROM `mydataset.MYTABLE_TEMP.nps_forecast_mes_corrigido`)
    -- ;
    -- -- demais execucoes: adiciona dados
    -- INSERT INTO `mydataset.MYTABLE_TEMP.NPS_PREDICAO` 
    -- SELECT * FROM `mydataset.MYTABLE_TEMP.nps_forecast_semana_corrigido`
    -- ORDER BY data_atualizacao, id
    -- ;
    -- INSERT INTO `mydataset.MYTABLE_TEMP.NPS_MENSAL_DASHBOARD` 
    -- SELECT * FROM `mydataset.MYTABLE_TEMP.nps_forecast_mes_corrigido`
    -- ORDER BY 1,2
    -- ; 
    -- cria tabela para dashboard predicao semanal
    CREATE OR REPLACE TABLE `mydataset.MYTABLE_TEMP.NPS_SEMANAL_DASHBOARD` AS (

    SELECT * EXCEPT (rank) FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY data_atualizacao ORDER BY id desc) AS rank
      FROM `mydataset.MYTABLE_TEMP.NPS_PREDICAO` 
    )
    WHERE rank > 2
    )
    ORDER BY 6,1
    ;

    """
    
    client.query(query_to_bq)


def history_semanal_parquet():
    
    """
    Função para adicionar dados do BQ para parquet no Storage.
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_history_semanal = f"""
    
    SELECT *
    FROM `mydataset.MYTABLE_PROD.NPS_PREDICAO` 

    """
    
    # Execute a query
    df_ = client.query(query_history_semanal).to_dataframe()
       
    return df_

def history_mensal_parquet():
    
    """
    Função para adicionar dados do BQ para parquet no Storage.
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_history_mensal = f"""
    
    SELECT *
    FROM `mydataset.MYTABLE_PROD.NPS_MENSAL_DASHBOARD` 

    """
    
    # Execute a query
    df_ = client.query(query_history_mensal).to_dataframe()
       
    return df_

def save_history(df, destination_file_name, folder, savedate):
    
    df.to_parquet(folder + f'gold/{destination_file_name}_{savedate}.parquet')


def main(folder, model_name):
    """
    Função principal para executar os processos de predicao_mensal.
    """
    # Fixar savedate para todos os modulos
    savedate = str(dt.date.today())[:10]
    
    print('Iniciando o processo save_history_bq')
    ini = time.time()
    save_history_bq()
    fim = time.time()
    print(f'Finalizado o processo save_history_bq em {(fim-ini):.2f} segundos')
    print('mydataset.MYTABLE_TEMP.NPS_SEMANAL_DASHBOARD')
    print()
    
    folder = folder + f'{model_name}_prediction/'   
    destination_file_name = f'{model_name}_forecast_semana_history' 
    
    print('Iniciando o processo history_semanal_parquet')
    ini = time.time()
    df  = history_semanal_parquet()
    fim = time.time()
    print(f'Finalizado o processo history_semanal_parquet em {(fim-ini):.2f} segundos')
    
    print('Iniciando o processo save_history')
    ini = time.time()
    save_history(df, destination_file_name, folder, savedate)
    fim = time.time()
    print(f'Finalizado o processo save_history em {(fim-ini):.2f} segundos')
    
    print(folder+destination_file_name)
    print('mydataset.MYTABLE_PROD.NPS_PREDICAO')
    print()
      
    destination_file_name = f'{model_name}_forecast_mes_history'
    
    print('Iniciando o processo history_mensal_parquet')
    ini = time.time()
    df  = history_mensal_parquet()
    fim = time.time()
    print(f'Finalizado o processo history_mensal_parquet em {(fim-ini):.2f} segundos')
    
    print('Iniciando o processo save_history')
    ini = time.time()
    save_history(df, destination_file_name, folder, savedate)
    fim = time.time()
    print(f'Finalizado o processo save_history em {(fim-ini):.2f} segundos')
    
    print(folder+destination_file_name)
    print('mydataset.MYTABLE_PROD.NPS_MENSAL_DASHBOARD')
    print()


folder = 'gs://myproject/Projeto_NPS/production/'
model_name = 'nps'

main(folder, model_name)

print('end of job')


# In[ ]:




