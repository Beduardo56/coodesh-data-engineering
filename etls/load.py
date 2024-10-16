import logging
import glob
import os

import pandas as pd
import boto3
from botocore.exceptions import ClientError

def upload_vendas_por_dia(data: pd.DataFrame, save_s3: bool = False,
                          bucket: str = "bucket-teste"):
    """Função responsável por salvar os dados de vendas por dia em formato parquet.
    Os dados vão ser salvos particionados por Mês e Ano e vão ser salvos no formato:
    vendas_totais/ano={x}/mes={y}/<hash>.parquet

    Args:
        data (pd.DataFrame): Dado de venda a ser salvo
        save_s3 (bool, optional): Se essa váriavel for verdadeira, tenta salvar no s3. Defaults to False.
        bucket (str, optional): Qual o bucket que vamos salvar os dados. Defaults to "bucket-teste".
    """
    if not data.empty:
        # Por uma questão de tamanho de arquivo, irei um arquivo parquet para cada mês.
        folderpath = '/tmp/vendas_totais'
        # Salva os arquivos no formato vendas_totais/ano={x}/mes={y}/<hash>.parquet na /tmp
        data.to_parquet(folderpath, partition_cols=['ano', 'mes'])
        # Se fosse salvar no s3, essa lógica salvaria o dado lá;
        if save_s3:
            files_path = glob.glob(folderpath)
            for file_path in files_path:
                object_name = file_path.rename('/tmp/', '')
                upload_file(file_path, bucket, object_name)

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    função copiada da documentação oficial do boto3: [https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html]
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


