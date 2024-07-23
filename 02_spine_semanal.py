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


def get_spine(model_name):
    
    """
    Função para construir a spine para a modelagem

    Parameters
    ----------
    model_name : str
        Nome do modelo.
    
    Returns
    -------
    df_: Dataframe
        Base resultante da query criada

    """
    
    query_spine = f"""

    WITH SPINE AS (
    SELECT
    monday,
    year,
    isoweek,
    COUNT(DISTINCT dataref) AS dias,
    COUNTIF(Classificacao='PROMOTOR') AS promotor,
    COUNTIF(Classificacao='NEUTRO') AS neutro,
    COUNTIF(Classificacao='DETRATOR') AS detrator,
    COUNT(*) AS respondentes,
    AVG(frac_sorteados) AS frac_sorteados,
    (COUNTIF(Classificacao='PROMOTOR')-COUNTIF(Classificacao='DETRATOR')) / COUNT(*) * 100 AS nps,
    CURRENT_DATE() AS data_atualizacao
    FROM `mydataset.MYTABLE_TEMP.nps_data`
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    )
    SELECT
    ROW_NUMBER() OVER(ORDER BY year, isoweek) AS id, 
    SPINE.*,
    (respondentes*7.5/frac_sorteados)*7/dias AS respondentes_ajustado
    FROM SPINE
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12
    ORDER BY 1,2,3,4

    """    
  
    # Execute a query
    df_ = client.query(query_spine).to_dataframe()
       
    return df_

def save_spine(df, destination_table, destination_file_name, folder, savedate):
    
    pdg.to_gbq(df, destination_table, project_id=your_project_id, credentials=credentials, if_exists='replace') 
    
    df.to_parquet(folder + f'silver/{destination_file_name}_{savedate}.parquet')


def main(folder, model_name):
    """
    Função principal para executar os processos de spine_semanal.
    """
    # Fixar savedate para todos os modulos
    savedate = str(dt.date.today())[:10]
    
    folder = folder + f'{model_name}_prediction/'   
    destination_file_name = f'{model_name}_spine' 
    destination_table = 'mydataset.MYTABLE_TEMP.' + destination_file_name
    
    print('Iniciando o processo get_spine')
    ini = time.time()
    df = get_spine(model_name)
    fim = time.time()
    print(f'Finalizado o processo get_spine em {(fim-ini):.2f} segundos')

    print('Iniciando o processo save_spine')
    ini = time.time()
    save_spine(df, destination_table, destination_file_name, folder, savedate)
    fim = time.time()
    print(f'Finalizado o processo save_spine em {(fim-ini):.2f} segundos')
    print()
    print(folder+destination_file_name)
    print(destination_table)

folder = 'gs://myproject/Projeto_NPS/production/'
model_name = 'nps'

main(folder, model_name)

print('end of job')


# In[ ]:




