#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas            as pd
import numpy             as np
# from datetime import timedelta
import datetime as dt
import time
import pandas_gbq as pdg
# from statsmodels.tsa.arima.model import ARIMA


from google.cloud import bigquery
from google.cloud import bigquery_storage
import google.auth

credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=your_project_id)


def forecasting():
    
    """
    Função para fazer a predição mensal a partir da semanal
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_fcst_month = f"""
    
    WITH REAL AS (
    SELECT
    CURRENT_DATE() AS data_atualizacao,
    DATE_TRUNC(DATE(dataref), MONTH) AS mes,
    (COUNTIF(Classificacao='PROMOTOR')-COUNTIF(Classificacao='DETRATOR')) / COUNT(*) * 100 AS nps_real,
    FROM `mydataset.MYTABLE_TEMP.nps_data`
    WHERE DATE_TRUNC(DATE(dataref), MONTH)<DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY 1,2
    ORDER BY 1,2
    )
    , FORECAST_AUX AS (
    SELECT data_atualizacao,DATE_TRUNC(DATE(monday), MONTH) AS mes,nps,respondentes_ajustado,forecast_value,respondentes_fcst,
    COALESCE(nps,forecast_value) AS nps_mes,
    COALESCE(respondentes_ajustado,respondentes_fcst) AS respondentes_mes,
    COALESCE(nps,forecast_value)*COALESCE(respondentes_ajustado,respondentes_fcst) AS npsXresp_mes
    FROM `mydataset.MYTABLE_TEMP.nps_forecast_semana`
    )
    , FORECAST AS (
    SELECT DATE(data_atualizacao) AS data_atualizacao, mes, SUM(npsXresp_mes)/SUM(respondentes_mes) AS nps_fcst
    FROM FORECAST_AUX
    WHERE mes=DATE_TRUNC(DATE(data_atualizacao), MONTH)
    GROUP BY 1,2
    ORDER BY 1,2
    )
    SELECT data_atualizacao,mes,nps_real,nps_fcst
    FROM REAL 
    FULL OUTER JOIN FORECAST USING (data_atualizacao,mes)
    ORDER BY 1,2    

    """    
  
    # Execute a query
    df_ = client.query(query_fcst_month).to_dataframe()
       
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
    df_['data_atualizacao'] = pd.to_datetime(df_['data_atualizacao']).dt.strftime('%Y-%m-%d')
    df_['mes'] = pd.to_datetime(df_['mes']).dt.strftime('%Y-%m-%d')
    
    return df_


def save_forecasts(df, destination_table, destination_file_name, folder, savedate):
    
    pdg.to_gbq(df, destination_table, project_id=your_project_id, credentials=credentials, if_exists='replace') 
    
    df.to_parquet(folder + f'gold/{destination_file_name}_{savedate}.parquet')


def main(folder, model_name):
    """
    Função principal para executar os processos de predicao_mensal.
    """
    # Fixar savedate para todos os modulos
    savedate = str(dt.date.today())[:10]
    
    folder = folder + f'{model_name}_prediction/'   
    destination_file_name = f'{model_name}_forecast_mes' 
    destination_table = 'mydataset.MYTABLE_TEMP.' + destination_file_name
    
    print('Iniciando o processo forecasting')
    ini = time.time()
    df_ = forecasting()
    fim = time.time()
    print(f'Finalizado o processo forecasting em {(fim-ini):.2f} segundos')
    
    print('Iniciando o processo correct_dtypes')
    ini = time.time()
    df = correct_dtypes(df_)
    fim = time.time()
    print(f'Finalizado o processo correct_dtypes em {(fim-ini):.2f} segundos')

    print('Iniciando o processo save_forecasts')
    ini = time.time()
    save_forecasts(df, destination_table, destination_file_name, folder, savedate)
    fim = time.time()
    print(f'Finalizado o processo save_forecasts em {(fim-ini):.2f} segundos')
    print()
    print(folder+destination_file_name)
    print(destination_table)

folder = 'gs://myproject/Projeto_NPS/production/'
model_name = 'nps'

main(folder, model_name)

print('end of job')


# In[ ]:




