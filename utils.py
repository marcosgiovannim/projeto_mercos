import json 
import os
import arrow
import mysql.connector
import pandas as pd

from loguru import logger


def get_connection():
    """
    Retorna uma conexão com o banco de dados.
    
    Returns:
        Connection: Conexão com o banco de dados.
    """
    try:
        conn = mysql.connector.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            port=os.environ.get('DB_PORT', 3306),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        return conn
    
    except mysql.connector.Error as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")   


def execute_query(conn, query):
    """
    Executa uma query SQL na conexão especificada.
    
    Args:
        conn: Conexão com o banco de dados.
        query: Query SQL a ser executada.
        
    Returns:
        list: Resultado da query.
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        return result


def truncate_table(conn, table):
    """
    Trunca a tabela especificada, mantendo a estrutura da tabela.
    
    Args:
        conn: Objeto de conexão com o banco de dados a ser usado na operação.
        table (str): Nome da tabela a ser truncada.
        
    Returns:
        list: Resultado da operação de truncamento (tipicamente vazio para consultas de truncamento).
    """
    query = f"""
        TRUNCATE TABLE {table};
    """
    return execute_query(conn, query)

def insert_data(conn, table, df):
    """
    Insere dados de um DataFrame em uma tabela do banco de dados.
    
    Esta função processa o DataFrame e insere seus dados na tabela especificada,
    usando inserção em lotes para melhor performance. Trata valores nulos
    e fornece feedback sobre o número de registros inseridos.
    
    Args:
        conn: Conexão com o banco de dados.
        table (str): Nome da tabela onde os dados serão inseridos.
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        
    Returns:
        None: A função não retorna valores, mas registra o resultado da operação.
    """
    cursor = conn.cursor()
    
    columns = ", ".join([f"`{col}`" for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))

    query = f"""
        INSERT INTO {table} ({columns}) VALUES ({placeholders});
    """

    # Converter dados do DataFrame para lista de valores, tratando valores nulos
    values = []
    for row in df.itertuples(index=False):
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append(None)
            else:
                row_values.append(val)
        values.append(row_values)

    # Definir tamanho do lote para inserção em massa
    batch_size = 1000
    total_inserted = 0
    
    # Inserir dados em lotes para melhor performance
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]
        cursor.executemany(query, batch)
        total_inserted += cursor.rowcount
        conn.commit()
    
    logger.success(f"{total_inserted} registros inseridos na tabela {table}")




def read_json_file(file_path):
    """
    Lê e analisa um arquivo JSON.
    Esta função tenta ler e analisar o arquivo JSON no caminho especificado.
    Registra quaisquer erros encontrados durante o processo.
    Args:
        file_path (str): Caminho para o arquivo JSON a ser lido.
    Returns:
        dict ou list: Dados JSON analisados se for bem-sucedido, None caso contrário.
    Raises:
        FileNotFoundError: Registra erro se o arquivo não for encontrado.
        json.JSONDecodeError: Registra erro se o arquivo não for um JSON válido.
    """
    try:
        with open(file_path) as f:
            data = json.load(f) 
            return data
        
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {file_path}");
    except json.JSONDecodeError:
        logger.error(f"Arquivo JSON inválido: {file_path}");


def list_files_directory(directory_path):
    """
    Lista todos os arquivos em um diretório especificado.
    
    Args:
        directory_path (str): Caminho para o diretório a ser listado.
        
    Returns:
        list: Lista com os caminhos completos de todos os arquivos no diretório.
        
    Raises:
        FileNotFoundError: Registra erro se o diretório não for encontrado.
    """
    try:
        # Verifica se o caminho é um diretório válido
        if not os.path.isdir(directory_path):
            raise FileNotFoundError 
    
        paths = []
        # Percorre todos os elementos no diretório
        for file_name in os.listdir(directory_path):
            full_path = os.path.join(directory_path, file_name)
            # Adiciona à lista apenas se for um arquivo (não diretório)
            if os.path.isfile(full_path):
                paths.append(full_path)
        return paths
    
    except FileNotFoundError:
        logger.error(f"Diretório não encontrado: {directory_path}");


def read_raw_files(directory_path="data/raw"):
    """
    Lê todos os arquivos JSON brutos de um diretório especificado.
    
    Args:
        directory_path (str): Caminho para o diretório contendo os arquivos brutos.
                             O valor padrão é "data/raw".
    
    Returns:
        list: Lista de tuplas contendo (nome_arquivo, dados) para cada arquivo JSON processado com sucesso.
    """
    # Obtém a lista de caminhos de arquivos no diretório
    paths = list_files_directory(directory_path)
    result = []

    # Processa cada arquivo encontrado
    for path in paths:
        file_name = os.path.basename(path)
        if file_name.endswith(".json"):
            # Lê o conteúdo do arquivo JSON
            data = read_json_file(path) 
            if data:  # Verifica se os dados não são None
                result.append((file_name, data))  # Adiciona tupla com o nome do arquivo e os dados
        else:
            logger.warning(f"Arquivo não suportado: {file_name}")

    return result


def convert_to_df(data: dict):  
    """
    Converte um dicionário em um DataFrame do pandas.
    
    Args:
        data (dict): Dicionário a ser convertido em DataFrame.
        
    Returns:
        pd.DataFrame: DataFrame criado a partir do dicionário.
        
    Raises:
        Exception: Registra erro se a conversão falhar.
    """
    try:
        # Cria um DataFrame usando as chaves do dicionário como índices
        df = pd.DataFrame.from_dict(data, orient='index')
        return df
    except Exception as e:
        # Registra qualquer erro ocorrido durante a conversão
        logger.error(f"Erro ao converter dados para DataFrame: {e}")


def convert_to_datetime(df: pd.DataFrame, column):
    
    """
    Converte uma coluna de um DataFrame para o formato datetime padrão.
    
    Args:
        df (pd.DataFrame): DataFrame contendo a coluna a ser convertida
        column: Nome da coluna que contém os valores de data a serem convertidos
        
    Returns:
        pd.Series: Série do pandas contendo as datas convertidas para formato datetime
        
    Raises:
        Exception: Registra erro se a conversão falhar
    """
    try:    
        
        def parse_and_standardize_date(value):
            """
            Função interna que analisa e padroniza valores de data em formato YYYY-MM-DD.
            Lida com diversos formatos de entrada incluindo YYYY-MM-DD, DD/MM/YYYY e outros.
            """
            try:
                # Retorna None para valores vazios ou nulos
                if pd.isna(value) or value is None or value == '':
                    return None
                    
                # Verifica se já está no formato desejado (YYYY-MM-DD)
                if isinstance(value, str) and '-' in value:
                    parts = value.split('-')
                    if len(parts) == 3 and len(parts[0]) == 4:
                        return value
                        
                # Processa datas no formato com barras (/)
                if isinstance(value, str) and '/' in value:
                    parts = value.split('/')
                    
                    if len(parts) == 3:
                        # Verifica se é YYYY/MM/DD (primeiro elemento tem 4 dígitos)
                        if len(parts[0]) == 4:
                            # Converte YYYY/MM/DD para YYYY-MM-DD
                            return f"{parts[0]}-{parts[1]}-{parts[2]}"
                            
                        # Verifica se é DD/MM/YYYY (último elemento tem 4 dígitos)
                        elif len(parts[2]) == 4:
                            # Reorganiza DD/MM/YYYY para YYYY-MM-DD
                            return f"{parts[2]}-{parts[1]}-{parts[0]}"
                
                # Para outros formatos, usa a biblioteca arrow para tentar converter
                date_obj = arrow.get(value)
                return date_obj.format('YYYY-MM-DD')
                
            except Exception as e:
                logger.error(f"Erro ao analisar data '{value}': {e}")
                return None

        # Aplica a função de conversão em cada elemento da coluna e converte para datetime
        result = pd.to_datetime(df[column].apply(parse_and_standardize_date)) 
        
        return result      

    except Exception as e:
        # Registra erro se houver falha geral na conversão da coluna
        logger.error(f"Erro ao converter colunas para datetime: {e}")