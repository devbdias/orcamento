from src.scripts.auth import *
from src.scripts.consultas import *
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
import numpy as np
import os
import re

import logging

logging.basicConfig(filename='execution_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def limpa_parquet(path):
    try:
        arquivos_parquet = [arquivo for arquivo in os.listdir(path) if arquivo.endswith('.parquet')]
        for arquivo in arquivos_parquet:
            os.remove(os.path.join(path, arquivo))
    except Exception as e:
        print(f"Erro ao limpar os arquivos Parquet em {path}: {e}")

def limpa_log(path_log):
    try:
        with open(path_log, 'wb') as log_file:
            log_file.truncate(0)
    except Exception as e:
        print(f"Erro ao limpar o log em {path_log}: {e}")

def conectar_banco_dados(server, database, username, password):
    try:
        conn_str = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return None

def executar_consulta(engine, sql_query):
    try:
        df = pd.read_sql_query(sql_query, engine)
        return df
    except Exception as e:
        print("Erro ao executar a consulta:", e)
        return None

def definir_consulta():
    try:
        consultas = [dim_unidade, dim_supervisor, dim_vendedor, dim_cliente, dim_produto, fato_vendas, fato_volume_orcado, fato_churn, ]
        return consultas
    except Exception as e:
        print("Erro ao executar consultas:", e)

def trata_linha(engine, consulta, path):
    try:
        if engine:
            df = executar_consulta(engine, consulta)
            if df is not None:
                if consulta == fato_vendas:
                    mask = df['LINHA'] == 'REVENDA'
                    df.loc[mask, 'FAMILIA'] = 'REVENDA'
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\7 FATO VENDAS.parquet', index=False)
                elif consulta == dim_unidade:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\1 DIM UNIDADE.parquet', index=False)
                elif consulta == dim_supervisor:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\2 DIM SUPERVISOR.parquet', index=False)
                elif consulta == dim_vendedor:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\3 DIM VENDEDOR.parquet', index=False)
                elif consulta == dim_cliente:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\4 DIM CLIENTE.parquet', index=False)
                elif consulta == dim_produto:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\5 DIM PRODUTO.parquet', index=False)
                elif consulta == fato_volume_orcado:
                    df['BUDGET VOLUME'] = df.apply(lambda row: row['BUDGET VOLUME'] * (0.2 / 0.33) if row['FAMILIA'] == 'FERMENTO LIQUIDO' else row['BUDGET VOLUME'], axis=1)
                    df['BUDGET VOLUME'] = df.apply(lambda row: row['BUDGET VOLUME'] * 3 if row['FAMILIA'] in ['LEVEDURA SECA', 'FI 10G', 'FI 500G'] else row['BUDGET VOLUME'], axis=1)
                    #df['BUDGET OBJETIVO'] = df.apply(lambda row: row['BUDGET OBJETIVO'] * (0.2 / 0.33) if row['FAMILIA'] == 'FERMENTO LIQUIDO' else row['BUDGET OBJETIVO'], axis=1)
                    #df['BUDGET OBJETIVO'] = df.apply(lambda row: row['BUDGET OBJETIVO'] * 3 if row['FAMILIA'] in ['LEVEDURA SECA', 'FI 10G', 'FI 500G'] else row['BUDGET OBJETIVO'], axis=1)
                    #df['BUDGET CAMPANHA'] = df.apply(lambda row: row['BUDGET CAMPANHA'] * (0.2 / 0.33) if row['FAMILIA'] == 'FERMENTO LIQUIDO' else row['BUDGET CAMPANHA'], axis=1)
                    #df['BUDGET CAMPANHA'] = df.apply(lambda row: row['BUDGET CAMPANHA'] * 3 if row['FAMILIA'] in ['LEVEDURA SECA', 'FI 10G', 'FI 500G'] else row['BUDGET CAMPANHA'], axis=1)
                    
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\8 FATO VOLUME ORCADO.parquet', index=False)
                elif consulta == fato_churn:
                    df.drop_duplicates(inplace=True)
                    df.to_parquet(fr'{path}\9 FATO CHURN.parquet', index=False)
                else:
                    pass
                engine.dispose()
    except Exception as e:
        print("Erro ao tratar linha:", e)

def criar_dimensao_calendario(data_inicio, data_fim, path_exportacao):
    try:
        periodo_datas = pd.date_range(data_inicio, data_fim)
        dim_calendario = pd.DataFrame()

        dim_calendario['data'] = periodo_datas
        dim_calendario['ano'] = dim_calendario['data'].dt.year
        dim_calendario['mes'] = dim_calendario['data'].dt.month
        dim_calendario['dia'] = dim_calendario['data'].dt.day
        dim_calendario['semana_do_ano'] = dim_calendario['data'].dt.isocalendar().week
        dim_calendario['dia_da_semana'] = dim_calendario['data'].dt.dayofweek
        dim_calendario['nome_dia_da_semana'] = dim_calendario['data'].dt.day_name()
        dim_calendario['nome_do_mes'] = dim_calendario['data'].dt.month_name()
        dim_calendario['trimestre'] = dim_calendario['data'].dt.quarter
        dim_calendario['ano_mes'] = dim_calendario['data'].dt.strftime('%Y%m')
        dim_calendario.to_parquet(path_exportacao, index=False)
    except Exception as e:
        print("Erro ao criar a dimensão de calendário:", e)

def main():
    try:
        
        data_path = os.path.join( "src", "data", "ready")
        log_dir   = os.path.join( "src", "logs")
        
        os.makedirs(data_path, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        log_path = os.path.join(log_dir, "execution_log.log")

        # Configuração do logger
        logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

        engine = conectar_banco_dados(server, database, username, password)
        if engine is not None:

            limpa_log(log_path)
            limpa_parquet(data_path)

            hoje = dt.datetime.today()
            ano_atual = hoje.year
            ultimo_dia_do_ano = dt.datetime(ano_atual, 12, 31)
            criar_dimensao_calendario('2022-01-01', f'{ultimo_dia_do_ano}', os.path.join(data_path, '6 DIM CALENDARIO.parquet'))
            consultas = definir_consulta()

            with open(log_path, 'r') as log_file:
                logged_lines = log_file.readlines()
                if logged_lines:
                    last_line = logged_lines[-1]
                    if "Erro ao executar a main" in last_line:
                        last_successful_index = consultas.index(last_line.split(':')[1].strip())
                        consultas = consultas[last_successful_index + 1:]
                        logging.info("\nReiniciando a partir da última consulta bem-sucedida: %s", consultas[0])

            for consulta in consultas:
                trata_linha(engine, consulta, data_path)
                logging.info("\nConsulta %s concluída com sucesso.", consulta)
            logging.info("\nExecução concluída com sucesso.")
                        
    except Exception as e:
        logging.error("Erro ao executar a main: %s", e)

if __name__ == "__main__":
    main()