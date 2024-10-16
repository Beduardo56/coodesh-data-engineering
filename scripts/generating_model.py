from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import xgboost as xgb
from etls import extract, transform
from sklearn.metrics import mean_absolute_error
import pickle

# Função útil para criar feature para séries temporais
def create_features(df, label=None):
    """
    Creates time series features from datetime index
    """
    df['date'] = df.index
    df['hour'] = df['date'].dt.hour
    df['dayofweek'] = df['date'].dt.dayofweek
    df['quarter'] = df['date'].dt.quarter
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['dayofyear'] = df['date'].dt.dayofyear
    df['dayofmonth'] = df['date'].dt.day
    df['weekofyear'] = df['date'].dt.weekofyear
    
    X = df[['hour','dayofweek','quarter','month','year',
           'dayofyear','dayofmonth','weekofyear']]
    if label:
        y = df[label]
        return X, y
    return X
# Buscando os dados de venda por dia.
data = extract.extract_vendas()
sales_data = transform.transform_vendas(data)
#Preparando sales_data para receber as novas features, precisamos da colunas "date"
sales_data['date'] = sales_data['data_venda'].apply(lambda x: datetime.fromisoformat(x))
# Para não haver vazamento de dados e isso estragar nosso modelo,
# os dados de validação vão representar os 20% dados mais novos
# (isso é essencial para séries temporais, já que não queremos que o consumo do passado seja predito por um consumo do futuro)
sales_data.sort_values(by=['date'], inplace=True)
X_train = sales_data[:int(0.8*len(sales_data))]
X_val = sales_data[int(0.8*len(sales_data)):]
# Criando datasets de treino e validação, usaremos uma validação simples para evitar o overfitting.
Xtrain, ytrain = create_features(X_train.set_index('date'), label='quantidade')
Xval, yval = create_features(X_val.set_index('date'), label='quantidade')
# Decidi usar o modelo de regressão xgbbost por alguns motivos:
# - Não precisamos fazer muito pré-processamento e tratamento de features.
# - Não precisamos otimizar os hiperparâmetros do modelo para obter um resultado bom e que entrega resultado.
# - Tem validação acoplada na função.
reg = xgb.XGBRegressor(n_estimators=2000, learning_rate=0.1, max_depth=5, colsample_bytree=0.1, eval_metric='mae')
reg.fit(Xtrain, ytrain,
        eval_set=[(Xtrain, ytrain), (Xval, yval)])
p1 = reg.predict(Xval)
print(mean_absolute_error(yval.values, reg.predict(Xval)))
file_name = "sales_xgb_regressor.pkl"
pickle.dump(reg, open(file_name, 'wb'))