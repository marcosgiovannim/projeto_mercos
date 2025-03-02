import os
import pyarrow as pa
from loguru import logger

from engine import RateioEngine

from utils import (
    read_raw_files,
    convert_to_df,
    convert_to_datetime
)

def load_json_data():
    """
    This function loads the data from the raw data directory.
    """
    logger.info("Lendo arquivos JSON")
    
    # Reading the JSON files from the raw data directory
    results = read_raw_files()
   
    df_dict = {}

    # Converting the JSON data to DataFrames
    for file_name, data in results:
        key = os.path.splitext(file_name)[0]
        df_dict[key] = convert_to_df(data)

    return df_dict

def prepare_dataframes(df_lancamentos, df_metricas):
    """Prepara e padroniza os DataFrames para processamento"""
    # Conversão de todas as colunas de data
    date_columns_list = [
        (df_lancamentos, ['dt_vencimento', 'dt_pagamento', 'dt_competencia']),
        (df_metricas, ['dt_referencia'])
    ]
    
    for df, columns in date_columns_list:
        for column in columns:
            df[column] = convert_to_datetime(df, column)
    
    return df_lancamentos, df_metricas

def main():

    # Reading the JSON files from the raw data directory
    df_dict = load_json_data() 
    df_lancamentos = df_dict.get("lancamentos")
    df_metricas = df_dict.get("metricas")

    # Prepara DataFrames convertendo colunas de data para datetime
    df_lancamentos, df_metricas = prepare_dataframes(df_lancamentos, df_metricas)

    # Cria e executar engine de rateio
    engine = RateioEngine(df_lancamentos, df_metricas)

    # Executar estágios
    stage1_df = engine.process_first_stage()
    stage2_df = engine.process_second_stage(stage1_df)
    
    # Salvar resultados
    stage1_df.to_parquet("data/processed/rateio_etapa1.parquet")
    stage2_df.to_parquet("data/processed/rateio_etapa2.parquet")

    logger.success("Processamento de dados completo")

    stage1_df.to_csv("data/processed/rateio_etapa1.csv")
    stage2_df.to_csv("data/processed/rateio_etapa2.csv")
    
    
if __name__ == "__main__":
    main()