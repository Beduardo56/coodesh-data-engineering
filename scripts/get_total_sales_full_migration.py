"""Essa script tem o objetivo de rodar a extração completa dos dados
   do banco de dados coodesh-teste autogerado pelo script sqlite-data-generator.py,
   fazer sua transformação e fazer um upload para o s3 usando arquivos parquet particionados por mes e ano.
"""
from etls import extract, transform, load

data = extract.extract_vendas()
sales_data = transform.transform_vendas(data)
load.upload_vendas_por_dia(sales_data)
