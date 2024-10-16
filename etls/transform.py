from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

def transform_vendas(data: pd.DataFrame) -> pd.DataFrame:
    """Função responsável por tranformar os dados de vendas
    vindos do banco de dados em quantidade de vendas por dia.

    Args:
        data (pd.DataFrame): Vendas vindo do banco de dados

    Returns:
        pd.DataFrame: Vendas por dia.
    """
    if not data.empty:
        try:
        # Transformando coluna data_venda para o formato ISO.
            data['data_venda'] = data['data_venda'].map(lambda x: datetime.strptime(x, '%Y-%m-%d').isoformat())
            # Removendo duplicatas
            data.drop_duplicates(subset=['id_produto', 'data_venda', 'id_cliente', 'quantidade'], inplace=True)
            # Calculando o total de vendas por dia e a quantidade de dinheiro ganho total.
            daily_sales = data.groupby(by=["data_venda"])[["quantidade", "valor_total"]].sum()
            # Resetando indice e fazendo data_venda retornar como uma coluna. 
            daily_sales.reset_index(inplace=True)
            
            daily_sales['data_venda_datetime'] = data['data_venda'].apply(lambda x: datetime.fromisoformat(x))
            daily_sales['ano'] = daily_sales['data_venda_datetime'].dt.year
            daily_sales['mes'] = daily_sales['data_venda_datetime'].dt.month
            daily_sales['dia'] = daily_sales['data_venda_datetime'].dt.day
            daily_sales.drop(columns=['data_venda_datetime'], inplace=True)
            return daily_sales
        except KeyError:
            logging.error("Dado está incompleto")
            raise Exception("Dado está incompleto")