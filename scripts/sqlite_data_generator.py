# importando bibliotecas
import sqlite3
import random
from datetime import datetime, timedelta

# Função que gera os dados que vamos usar no projeto.
def generate_sales_data(num_records=500):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    products = list(range(101, 111))  # 10 produtos
    regions = ['Norte', 'Sul', 'Leste', 'Oeste', 'Centro']
    
    sales_data = []
    for i in range(1, num_records + 1):
        sale_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        product_id = random.choice(products)
        customer_id = random.randint(1001, 2000)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10.0, 100.0), 2)
        total_value = round(quantity * unit_price, 2)
        seller_id = random.randint(1, 20)
        region = random.choice(regions)
        
        sale = (i, sale_date.strftime('%Y-%m-%d'), product_id, customer_id, quantity, unit_price, total_value, seller_id, region)
        sales_data.append(sale)
    
    return sales_data

# Criando a banco de dados a ser populado
conn = sqlite3.connect("coodesh-teste.db")

# Conectando com o banco de dados.
cursor = conn.cursor()

# Criando tabela de vendas
cursor.execute("""
CREATE TABLE IF NOT EXISTS vendas (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        data_venda DATE NOT NULL,
        id_produto INTEGER NOT NULL,
        id_cliente INTEGER NOT NULL,
        quantidade INTEGER NOT NULL,
        valor_unitario DECIMAL(10,2) NOT NULL,
        valor_total DECIMAL(10,2) NOT NULL,
        id_vendedor INTEGER NOT NULL,
        regiao VARCHAR(50) NOT NULL
);
""")

sales_data = generate_sales_data(500)
# Inserindo dados.
cursor.executemany("""INSERT INTO vendas (id, data_venda, id_produto, id_cliente, quantidade, valor_unitario, valor_total, id_vendedor, regiao)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   """, sales_data)
conn.commit()

print('Dados inseridos com sucesso.')
cursor.close()