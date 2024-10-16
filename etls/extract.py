from datetime import datetime, timezone, timedelta
import sqlite3
import pandas as pd
import logging
from sqlite3 import DatabaseError

def extract_vendas(begin: datetime = None, end: datetime = None) -> pd.DataFrame:
    """Função responsável por extrair os dados do banco de dados,
    podemos passar o período que queremos extrair os dados.

    Args:
        begin (datetime, optional): Data de início do filtro. Defaults to None.
        end (datetime, optional): Data de fim do filtro. Defaults to None.

    Returns:
        pd.DataFrame: Dados de vendas extraidos do banco
    """
    try:
        if begin is not None:
            begin_date = begin.strftime("%Y-%m-%d")
        if end is not None:
            end_date = end.strftime("%Y-%m-%d")
    except AttributeError:
        logging.error("Não foi passado begin ou end no formato certo")
        raise Exception("Begin ou end passados erroneamente")
    conn = sqlite3.connect("coodesh-teste.db")
    try:
        # Caso onde passamos o inicio e fim.
        if begin is not None and end is not None:
            data = pd.read_sql(f"SELECT * FROM vendas WHERE data_venda >= {begin_date} and data_venda <= {end_date}", conn)
        # Caso onde só passamos o fim.
        elif begin is None and end is not None:
            data = pd.read_sql(f"SELECT * FROM vendas WHERE data_venda <= {end_date}", conn)
        # Caso onde só passamos o inicio.
        elif begin is not None and end is None:
            data = pd.read_sql(f"SELECT * FROM vendas WHERE data_venda >= {begin_date}", conn)
        # Caso sem restrição de datas
        else:
            data = pd.read_sql("SELECT * FROM vendas", conn)
    except DatabaseError:
        logging.error("Não foi criado o banco de dados")
        raise Exception("Não foi criado o banco de dados")

    return data
