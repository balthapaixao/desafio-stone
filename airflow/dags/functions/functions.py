
##Balthazar Paixão
#
##
###
####
###
##
#

##functions.py

import requests
import json
import numpy as np 
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker

def cleanJob(col):
    reps = {
    'valores_validos': ['Consultor Comercial Externo',
                        'Consultor Comercial',
                        'Agente Stone'],
    'valor_reposicao': np.nan
    }
    col = col.replace('Consultor Comercial  Externo', 'Consultor Comercial Externo')
    col = col.where(col.isin(reps['valores_validos']), reps['valor_reposicao'])
    return col

getCity = lambda lista: lista[-1].strip()
getJob = lambda lista: lista[-2].strip()


def extractTransformIbge():
    '''Retorna dataframe com municipios e estado segundo a API do IBGE para Merge'''
    api_response = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios").json()
    cidades = [municipio['nome'] for municipio in api_response]
    estados = [municipio['microrregiao']['mesorregiao']["UF"]["sigla"] for municipio in api_response]
    df_ibge = pd.DataFrame(data = {"municipios":cidades,
                                   'estados':estados})
    return df_ibge

def connectDB():
    db = 'postgresql+psycopg2'
    user = 'airflow'
    pwd = 'airflow'
    host = 'postgres' #'0.0.0.0'
    port = '5432'
    db_name = 'airflow'

    db_engine = create_engine(f"{db}://{user}:{pwd}@{host}:{port}/{db_name}")
    db_engine.connect()
    return db_engine

def LoadPostrges(df, db_name, engine):
    df.to_sql(db_name, engine, if_exists='replace')#if_exists='append' em caso de rotina 

def stoneETL():
    print("running extract...")
    df_inscritos = pd.read_csv("https://raw.githubusercontent.com/balthapaixao/desafio-stone/main/data/applications.csv", sep=";") 
    df_vagas = pd.read_csv("https://raw.githubusercontent.com/balthapaixao/desafio-stone/main/data/jobs.csv", sep=";")
    
    print("running transform...")
    splitted_job = df_vagas.job_name.str.split("-")
    df_vagas['cidade'] = [getCity(row) for row in splitted_job]
    df_vagas['cargo'] = [getJob(row) if len(row) > 1 else None for row in splitted_job]
    df_vagas['cargo'] = cleanJob(df_vagas['cargo'])
    #merge
    df_inscritosVagas = df_inscritos.merge(df_vagas, left_on='job_id', right_on='job_id').drop(columns=['job_name'])

    print("running load...")
    #fazendo conexão
    db_engine = connectDB()
    LoadPostrges(df_inscritosVagas, "stone", db_engine)
    
    print("ETL Stone COMPLETA")


def ibgeETL():
    print("running extract-transform...")
    df_ibge = extractTransformIbge()

    print("running load...")
    #fazendo conexão
    db_engine = connectDB()
    LoadPostrges(df_ibge, "ibge", db_engine)
    
    print("ETL IBGE COMPLETA")