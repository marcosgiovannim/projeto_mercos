import os
from loguru import logger

from engine import RateioEngine

from utils import (
    read_raw_files,
    convert_to_df,
    convert_to_datetime,
    get_connection,
    truncate_table,
    insert_data
)

def load_json_data():
    """
    Esta função carrega os dados do diretório de dados brutos e os converte em DataFrames.
    
    Processo:
    1. Lê todos os arquivos JSON do diretório de dados brutos
    2. Converte cada arquivo JSON em um DataFrame
    3. Armazena os DataFrames em um dicionário usando o nome do arquivo (sem extensão) como chave
    
    Returns:
        dict: Dicionário onde as chaves são os nomes dos arquivos e os valores são os DataFrames correspondentes
    """
    logger.info("Iniciando carregamento dos arquivos JSON do diretório de dados brutos")
    
    results = read_raw_files()
   
    df_dict = {}

    for file_name, data in results:
        key = os.path.splitext(file_name)[0]
        df_dict[key] = convert_to_df(data)

    return df_dict

def prepare_dataframes(df_lancamentos, df_metricas):
    """
    Prepara e padroniza os DataFrames para processamento, convertendo colunas de datas 
    para o formato datetime.
    Parameters:
    -----------
    df_lancamentos : pandas.DataFrame
        DataFrame contendo informações de lançamentos financeiros com colunas de data
        'dt_vencimento', 'dt_pagamento', e 'dt_competencia'.
    df_metricas : pandas.DataFrame
        DataFrame contendo métricas com coluna de data 'dt_referencia'.
    Returns:
    --------
    tuple
        Uma tupla contendo (df_lancamentos, df_metricas) com as colunas de data
        devidamente convertidas para o formato datetime.
    Notes:
    ------
    A função utiliza a função auxiliar 'convert_to_datetime' para realizar as conversões
    de formato de data.
    """
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
    
    conn = get_connection()

    # Barcos de dados : trunca e insere dados
    # OBS.: Manter comentado caso não tenha acesso ao banco de dados
    truncate_table(conn, "tb_rateio_1")
    truncate_table(conn, "tb_rateio_2")

    # OBS.: Manter comentado caso não tenha acesso ao banco de dados
    insert_data(conn, "tb_rateio_1", stage1_df)
    insert_data(conn, "tb_rateio_2", stage2_df)

    # Salvar resultados
    stage1_df.to_parquet("data/processed/rateio_etapa1.parquet")
    stage2_df.to_parquet("data/processed/rateio_etapa2.parquet")

    logger.success("Processamento de dados completo")

    
if __name__ == "__main__":
    main()