# Projeto Mercos

Este projeto realiza o rateio de valores entre diferentes centros e segmentos baseado em métricas específicas e critérios de alocação.

## Instalação

1. Clone o repositório:
    ```sh
    git clone https://github.com/marcosgiovannim/projeto_mercos.git
    cd projeto_mercos
    ```

2. Crie um ambiente virtual e ative-o:
    ```sh
    python -m venv venv
    source venv/bin/activate  # No Windows, use `venv\Scripts\activate`
    ```

3. Instale as dependências:
    ```sh
    pip install -r requirements.txt
    ```

## Uso

1. Coloque os arquivos JSON de dados brutos no diretório `data/raw`.

2. Execute o script principal:
    ```sh
    python main.py
    ```

3. Os resultados processados serão salvos no diretório `data/processed` como arquivos Parquet.

## Estrutura do Projeto

- `main.py`: Script principal que orquestra o carregamento, preparação e processamento dos dados.
- `engine.py`: Contém a classe `RateioEngine` responsável pelo processamento dos estágios de rateio.
- `utils.py`: Funções utilitárias para leitura de arquivos, conversão de dados e manipulação de DataFrames.
- `requirements.txt`: Lista de dependências do projeto.


