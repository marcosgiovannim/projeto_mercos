import pandas as pd
from loguru import logger


class RateioEngine:
    """
    Engine responsável pelo rateio de valores entre diferentes centros e segmentos
    baseado em métricas específicas e critérios de alocação.
    """

    def __init__(self, df_lancamentos, df_metricas):
        """
        Inicializa o motor de rateio com os dataframes necessários.
        
        Args:
            df_lancamentos: DataFrame com os lançamentos financeiros a serem rateados
            df_metricas: DataFrame com as métricas que serão base para o rateio
        """
        self.df_lancamentos = df_lancamentos
        self.df_metricas = df_metricas

    def __filter_by_period_and_criteria(self, df, date_column, months, **criteria):
        """
        Filtra DataFrame por período (meses) e critérios adicionais.
        
        Args:
            df: DataFrame a ser filtrado
            date_column: Nome da coluna de data
            months: Lista de meses para filtrar
            criteria: Critérios adicionais no formato coluna=valores
            
        Returns:
            DataFrame filtrado
        """
        mask = df[date_column].dt.month.isin(months)
        for column, values in criteria.items():
            mask &= df[column].isin(values)
        return df[mask].copy()

    def __calculate_allocation_ratios(self, df, group_columns, value_column='total'):
        """
        Calcula razões de alocação para cada grupo definido.
        
        Args:
            df: DataFrame com os dados para cálculo
            group_columns: Colunas para agrupamento
            value_column: Coluna contendo os valores para cálculo de proporção
            
        Returns:
            DataFrame com os ratios de alocação por grupo
        """
        totals = df.groupby(group_columns)[value_column].sum().reset_index()
        total_metric_value = totals['total'].sum()
        totals['allocation_ratio'] = totals['total'] / total_metric_value if total_metric_value > 0 else 0
        return totals
    
    def __create_allocation_records(self, lancamentos_df, allocation_df, value_column, ratio_column, stage):
        """
        Cria registros de alocação para cada combinação lançamento/alocação.
        
        Args:
            lancamentos_df: DataFrame com os lançamentos a serem alocados
            allocation_df: DataFrame com os ratios de alocação
            value_column: Nome da coluna com valores a serem alocados
            ratio_column: Nome da coluna com os ratios de alocação
            stage: Número da etapa de rateio
            
        Returns:
            DataFrame com todos os registros alocados
        """
        results = []
        for _, lancamento in lancamentos_df.iterrows():
            for _, row in allocation_df.iterrows():
                allocated_value = lancamento[value_column] * row[ratio_column]
                
                # Criar registro base com todas as colunas do lançamento
                record = lancamento.to_dict()
                
                # Adicionar/sobrescrever campos específicos de alocação
                record.update({
                    'valor_rateado': allocated_value,
                    'etapa_rateio': stage
                })
                
                # Adicionar campos do ratio se existirem
                for col in allocation_df.columns:
                    if col not in ['total', ratio_column]:
                        record[col] = row[col]
                
                results.append(record)
        
        return pd.DataFrame(results)

    def process_first_stage(self):
        """
        Processa o primeiro estágio de rateio, alocando valores dos centros de resultado
        específicos (100, 204) baseado em métricas por canal e segmento.
        
        Returns:
            DataFrame com os registros após o primeiro estágio de rateio
        """
        # Filtra dados relevantes para o primeiro rateio
        metricas_filtered = self.__filter_by_period_and_criteria(
            self.df_metricas,
            'dt_referencia', [10, 11],
            ds_metrica=['metrica_2'],
            ds_canal_aquisicao=['canalA', 'canalB']
        )

        centers_filtered = self.__filter_by_period_and_criteria(
            self.df_lancamentos,
            'dt_competencia', [10, 11],
            id_centro_resultado=[100, 204]
        )

        # Calcula ratios de alocação
        allocation_ratios = self.__calculate_allocation_ratios(
            metricas_filtered, 
            ['ds_canal_aquisicao', 'ds_segmento']
        )

        # Criar registros alocados
        allocated_records = self.__create_allocation_records(
            centers_filtered,
            allocation_ratios,
            'valor',
            'allocation_ratio',
            1
        )

        # Obter registros não alocados nesta etapa
        non_allocated = self.df_lancamentos[
            ~self.df_lancamentos['id_centro_resultado'].isin([100, 204])
        ].assign(
            valor_rateado=lambda x: x['valor'],
            ds_canal_aquisicao=None,
            ds_segmento=None,
            etapa_rateio=0
        )

        # Combinar resultados alocados e não alocados
        stage1_df = pd.concat([allocated_records, non_allocated], ignore_index=True)
        
        logger.success(f"Primeiro estágio concluído: {len(stage1_df)} registros")
        
        return stage1_df
    
    def process_second_stage(self, stage1_df):
        """
        Processa o segundo estágio de rateio, alocando valores dos centros de resultado
        específicos (268, 288) baseado em métricas por segmento.
        
        Args:
            stage1_df: DataFrame resultante do primeiro estágio de rateio
            
        Returns:
            DataFrame com os registros após o segundo estágio de rateio
        """
        # Filtra dados relevantes para o segundo rateio
        metricas_filtered = self.__filter_by_period_and_criteria(
            self.df_metricas,
            'dt_referencia', [10, 11],
            ds_metrica=['metrica_2']   
        )

        centers_filtered = self.__filter_by_period_and_criteria(
            stage1_df,
            'dt_competencia', [10, 11],
            id_centro_resultado=[268, 288]
        )

        # Calcula ratios de alocação por segmento
        allocation_ratios = self.__calculate_allocation_ratios(
            metricas_filtered, 
            ['ds_segmento']
        )

        # Criar registros alocados
        allocated_records = self.__create_allocation_records(
            centers_filtered,
            allocation_ratios,
            'valor',
            'allocation_ratio',
            2
        )

        # Obter registros já alocados no primeiro estágio
        already_allocated = stage1_df[stage1_df['etapa_rateio'] == 1]

        # Obter registros não alocados nesta etapa
        non_allocated_stage2 = stage1_df[
            (stage1_df['etapa_rateio'] == 0) & 
            (~stage1_df['id_centro_resultado'].isin([268, 288]))
        ]
        
        # Combinar todos os registros
        combined_df = pd.concat([already_allocated, allocated_records, non_allocated_stage2], ignore_index=True)
        
        logger.success(f"Segundo estágio concluído: {len(combined_df)} registros")
        
        return combined_df