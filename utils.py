import json 
import os
import arrow
import pandas as pd
from loguru import logger


def read_json_file(file_path):
    
    try:
        with open(file_path) as f:
            data = json.load(f) 
            return data
        
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {file_path}");
    except json.JSONDecodeError:
        logger.error(f"Arquivo JSON inválido: {file_path}");


def list_files_directory(directory_path):

    try:
        
        if not os.path.isdir(directory_path):
            raise FileNotFoundError 
    
        paths = []
        for file_name in os.listdir(directory_path):
            full_path = os.path.join(directory_path, file_name)
            if os.path.isfile(full_path):
                paths.append(full_path)
        return paths
    
    except FileNotFoundError:
        logger.error(f"Diretório não encontrado: {directory_path}");


def read_raw_files(directory_path="data/raw"):
    
    paths = list_files_directory(directory_path)
    result = []

    for path in paths:
        file_name = os.path.basename(path)
        if file_name.endswith(".json"):
            data = read_json_file(path) 
            if data: # Verify if the data is not None
                result.append((file_name, data)) # Tuple with the file name and the data
        else:
            logger.warning(f"Arquivo não suportado: {file_name}")

    return result


def convert_to_df(data: dict):  
    try:
        df = pd.DataFrame.from_dict(data, orient='index')
        return df
    except Exception as e:
        logger.error(f"Erro ao converter dados para DataFrame: {e}")


def convert_to_datetime(df: pd.DataFrame, column):
    
    try:    
        
        def parse_and_standardize_date(value):
            
            try:
                if pd.isna(value) or value is None or value == '':
                    return None
                    
                # Se já estiver no formato desejado (YYYY-MM-DD)
                if isinstance(value, str) and '-' in value:
                    parts = value.split('-')
                    if len(parts) == 3 and len(parts[0]) == 4:
                        # Já está no formato YYYY-MM-DD
                        return value
                        
                # Se estiver no formato com barras
                if isinstance(value, str) and '/' in value:
                    parts = value.split('/')
                    
                    if len(parts) == 3:
                        # Verifica se é YYYY/MM/DD (primeiro elemento tem 4 dígitos)
                        if len(parts[0]) == 4:
                            # Já está com o ano na frente, só trocar separadores
                            return f"{parts[0]}-{parts[1]}-{parts[2]}"
                            
                        # Verifica se é DD/MM/YYYY (último elemento tem 4 dígitos)
                        elif len(parts[2]) == 4:
                            # Reorganizar de DD/MM/YYYY para YYYY-MM-DD
                            return f"{parts[2]}-{parts[1]}-{parts[0]}"
                
                # Tenta converter com arrow caso não tenha sido tratado pelos casos acima
                date_obj = arrow.get(value)
                return date_obj.format('YYYY-MM-DD')
                
            except Exception as e:
                logger.error(f"Erro ao analisar data '{value}': {e}")
                return None


        result = pd.to_datetime(df[column].apply(parse_and_standardize_date)) 
        
        return result      

    except Exception as e:
        logger.error(f"Erro ao converter colunas para datetime: {e}")





    
    

