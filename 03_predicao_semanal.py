#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas            as pd
import numpy             as np
from datetime import timedelta
import datetime as dt
import time
import pandas_gbq as pdg
from statsmodels.tsa.arima.model import ARIMA

from google.cloud import bigquery
from google.cloud import bigquery_storage
import google.auth

credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=your_project_id)


def get_spine():
    
    """
    Função para carregar a spine para a predição
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_spine = f"""
    
    SELECT *
    FROM `mydataset.MYTABLE_TEMP.nps_spine`   

    """    
  
    # Execute a query
    df_ = client.query(query_spine).to_dataframe()
       
    return df_

def forecasting(df_spine):
    
    """
    Função para fazer os forecasts semanais
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    # corrigir dtypes
    df_spine['monday'] = pd.to_datetime(df_spine['monday']).dt.strftime('%Y-%m-%d')
    df_spine['data_atualizacao'] = pd.to_datetime(df_spine['data_atualizacao']).dt.strftime('%Y-%m-%d')
    df_spine['monday'] = pd.to_datetime(df_spine['monday']).dt.strftime('%Y-%m-%d')
    df_spine['id'] = df_spine['id'].astype(int)
    df_spine['year'] = df_spine['year'].astype(int)
    df_spine['isoweek'] = df_spine['isoweek'].astype(int)
    df_spine['dias'] = df_spine['dias'].astype(int)
    df_spine['promotor'] = df_spine['promotor'].astype(int)
    df_spine['neutro'] = df_spine['neutro'].astype(int)
    df_spine['detrator'] = df_spine['detrator'].astype(int)
    df_spine['respondentes'] = df_spine['respondentes'].astype(int)

    # criar 4 linhas no final para guardar os forecasts
    start=len(df_spine)

    for i in range(start, start+4):
        new_row = {
            'id': df_spine['id'].iloc[-1] + 1,
            'monday':  (pd.to_datetime(df_spine['monday'].iloc[-1]) + timedelta(days=7)).strftime('%Y-%m-%d'),
            'year': int((pd.to_datetime(df_spine['monday'].iloc[-1]) + timedelta(days=7)).strftime('%Y')),
            'isoweek': (pd.to_datetime(df_spine['monday'].iloc[-1]) + timedelta(days=7)).isocalendar()[1],
            'dias': 7,
            'promotor': np.nan,
            'neutro': np.nan,
            'detrator': np.nan,
            'respondentes': np.nan,
            'frac_sorteados': 7.5,
            'nps': np.nan,
            'data_atualizacao': df_spine['data_atualizacao'].iloc[-1],
            'respondentes_ajustado': np.nan
        }
        df_spine = df_spine.append(new_row, ignore_index=True)

    # forecasts
    df_ = df_spine.copy()
    treino = df_.copy()

    modelo_nps = ARIMA(treino['nps'], order=(3, 0, 1))
    resultado_nps = modelo_nps.fit(method_kwargs={'maxiter':300}) 
    previsoes_nps = resultado_nps.get_forecast(steps=4)
    intervalo_confianca_nps = previsoes_nps.conf_int(alpha=0.5)

    modelo_resp = ARIMA(treino['respondentes_ajustado'], order=(0,0,1))
    resultado_resp = modelo_resp.fit()
    previsoes_resp = resultado_resp.get_forecast(steps=4)
    intervalo_confianca_resp = previsoes_resp.conf_int(alpha=0.5)

    df_['forecast_value'] = np.nan
    df_['confidence_interval_lower_bound'] = np.nan
    df_['confidence_interval_upper_bound'] = np.nan
    df_['respondentes_fcst'] = np.nan

    df_.iloc[-4:, df_.columns.get_loc('forecast_value')] = previsoes_nps.predicted_mean.values
    df_.iloc[-4:, df_.columns.get_loc('confidence_interval_lower_bound')] = intervalo_confianca_nps.iloc[:, 0].values
    df_.iloc[-4:, df_.columns.get_loc('confidence_interval_upper_bound')] = intervalo_confianca_nps.iloc[:, 1].values
    df_.iloc[-4:, df_.columns.get_loc('respondentes_fcst')] = previsoes_resp.predicted_mean.values

    return df_

def save_forecasts(df, destination_table, destination_file_name, folder, savedate):
    
    pdg.to_gbq(df, destination_table, project_id=your_project_id, credentials=credentials, if_exists='replace') 
    
    df.to_parquet(folder + f'gold/{destination_file_name}_{savedate}.parquet')


def main(folder, model_name):
    """
    Função principal para executar os processos de predicao_semanal.
    """
    # Fixar savedate para todos os modulos
    savedate = str(dt.date.today())[:10]
    
    folder = folder + f'{model_name}_prediction/'   
    destination_file_name = f'{model_name}_forecast_semana' 
    destination_table = 'mydatset.MYTABLE_TEMP.' + destination_file_name
    
    print('Iniciando o processo get_spine')
    ini = time.time()
    df_spine = get_spine()
    fim = time.time()
    print(f'Finalizado o processo get_spine em {(fim-ini):.2f} segundos')
    
    print('Iniciando o processo forecasting')
    ini = time.time()
    df = forecasting(df_spine)
    fim = time.time()
    print(f'Finalizado o processo forecasting em {(fim-ini):.2f} segundos')

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

