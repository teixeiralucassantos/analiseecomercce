# Importar bibliotecas
import pandas as pd
import sqlite3
import os
# Importar bibliotecas para visualização
import matplotlib.pyplot as plt
import folium
from folium.plugins import MarkerCluster
import seaborn as sns
import numpy as np

# Caminho do arquivo CSV e do banco de dados
base_path = r'C:\Users\User\Documents\portfolio'
db_file_path = os.path.join(base_path, 'portfolio.db')

# Conectar ao banco de dados SQLite (cria o arquivo se não existir)
conn = sqlite3.connect(db_file_path)

# Criar a tabela consumidores
create_consumidores_table = """
CREATE TABLE IF NOT EXISTS consumidores (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);
"""

# Criar a tabela categoria
create_categoria_table = """
CREATE TABLE IF NOT EXISTS categoria (
    product_category_name TEXT,
    product_category_name_english TEXT
);
"""

# Criar a tabela vendedores
create_vendedores_table = """
CREATE TABLE IF NOT EXISTS vendedores (
    seller_id TEXT,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);
"""

# Criar a tabela geolocalização
create_geolocalizacao_table = """
CREATE TABLE IF NOT EXISTS geolocalizacao (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
);
"""

# Criar a tabela pagamentos
create_pagamentos_table = """
CREATE TABLE IF NOT EXISTS pagamentos (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL
);
"""

# Criar a tabela pedidos revisados
create_pedidos_revisados_table = """
CREATE TABLE IF NOT EXISTS pedidos_revisados (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);
"""

# Criar a tabela ordens
create_ordens_table = """
CREATE TABLE IF NOT EXISTS ordens (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);
"""

# Criar a tabela produtos (corrigindo o erro de digitação)
create_produtos_table = """
CREATE TABLE IF NOT EXISTS produtos (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght TEXT,  -- Correção aqui
    product_description_lenght TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT
);
"""
# Criar a tabela itens_pedidos
create_itens_pedidos_table = """
CREATE TABLE IF NOT EXISTS itens_pedidos (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price REAL,
    freight_value REAL
);
"""

# Executar a criação da tabela
conn.execute(create_itens_pedidos_table)

# Executar a criação das tabelas
conn.execute(create_consumidores_table)
conn.execute(create_categoria_table)
conn.execute(create_vendedores_table)
conn.execute(create_geolocalizacao_table)
conn.execute(create_pagamentos_table)
conn.execute(create_pedidos_revisados_table)
conn.execute(create_ordens_table)
conn.execute(create_produtos_table)

# Função para substituir valores que não são do tipo da maioria
def clean_column(df, column):
    # Verificar a maioria dos tipos de dados
    common_type = df[column].apply(type).mode()[0]
    
    # Substituir valores que não são do tipo comum ou são nulos
    df[column] = df[column].where(df[column].apply(lambda x: isinstance(x, common_type)), "")
    df[column] = df[column].fillna("")  # Substituir nulos por string vazia

# Função para ler e inserir dados em uma tabela
def read_and_insert_data(file_name, table_name):
    file_path = os.path.join(base_path, file_name)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"O arquivo {file_path} não foi encontrado.")
    
    df = pd.read_csv(file_path)

    # Limpar dados
    for column in df.columns:
        clean_column(df, column)
    
    # Inserir dados na tabela
    df.to_sql(table_name, conn, if_exists='append', index=False)

# Ler e inserir dados na tabela consumidores
consumers_file_name = 'olist_customers_dataset.csv'
df_consumers = pd.read_csv(os.path.join(base_path, consumers_file_name))
df_consumers.to_sql('consumidores', conn, if_exists='append', index=False)

# Inserir dados nas demais tabelas
read_and_insert_data('product_category_name_translation.csv', 'categoria')
read_and_insert_data('olist_sellers_dataset.csv', 'vendedores')
read_and_insert_data('olist_geolocation_dataset.csv', 'geolocalizacao')
read_and_insert_data('olist_order_payments_dataset.csv', 'pagamentos')
read_and_insert_data('olist_order_reviews_dataset.csv', 'pedidos_revisados')
read_and_insert_data('olist_orders_dataset.csv', 'ordens')
read_and_insert_data('olist_products_dataset.csv', 'produtos')
read_and_insert_data('olist_order_items_dataset.csv', 'itens_pedidos')



print("Tabelas criadas e dados inseridos com sucesso!")

# Rank top product categories by total sales 
query_1 = """
SELECT 
    c.product_category_name_english AS product_category,
    SUM(i.price * i.order_item_id) AS total_sales
FROM itens_pedidos i
LEFT JOIN produtos p ON p.product_id = i.product_id
LEFT JOIN categoria c ON c.product_category_name = p.product_category_name
GROUP BY c.product_category_name_english
ORDER BY total_sales DESC
LIMIT 15
"""

# Executar a consulta usando a conexão existente
top_product_categories = pd.read_sql_query(query_1, conn)
print(top_product_categories)

# Converter a coluna total_sales para float antes de substituir
top_product_categories['total_sales'] = top_product_categories['total_sales'].astype(float)

# Filtrar as 5 categorias mais vendidas e ordenar do maior para o menor
top_5_categories = top_product_categories.sort_values(by='total_sales', ascending=False).head(5)

# Criar gráfico
plt.figure(figsize=(10, 6))
bars = plt.bar(top_5_categories['product_category'], top_5_categories['total_sales'], color='navy')

# Adicionar rótulos de dados acima das barras com os valores de total_sales
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, f'R${yval:,.2f}'.replace('.', ','), ha='center', va='bottom', fontsize=10)

plt.title('As 5 Categorias de Produtos Mais Vendidos em R$', fontsize=16)
plt.xlabel('Categorias de Produtos', fontsize=14)
plt.ylabel('Total de Vendas (R$)', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Configurar fundo do gráfico e bordas
plt.gcf().patch.set_facecolor('lightgray')  # Fundo cinza claro para a figura
plt.gca().set_facecolor('lightgray')  # Fundo cinza claro para o eixo

plt.tight_layout()  # Ajustar layout
plt.show()

# Find monthly sales trend for the top 5 product categories with most sales
query_2 = """
WITH top_products AS (
    SELECT
        c.product_category_name_english AS category,
        SUM(i.price * i.order_item_id) AS sales
    FROM itens_pedidos i
    LEFT JOIN produtos p ON p.product_id = i.product_id
    LEFT JOIN categoria c ON c.product_category_name = p.product_category_name
    GROUP BY c.product_category_name_english
    ORDER BY sales DESC
    LIMIT 5
)
SELECT
    c.product_category_name_english AS category_name,
    strftime('%Y-%m', o.order_purchase_timestamp) AS purchase_year_month, --For sqlite3
    SUM(i.price * i.order_item_id) AS monthly_sales
FROM ordens o
LEFT JOIN itens_pedidos i ON o.order_id = i.order_id
LEFT JOIN produtos p ON p.product_id = i.product_id
LEFT JOIN categoria c ON c.product_category_name = p.product_category_name
WHERE c.product_category_name_english IN (SELECT category FROM top_products)
GROUP BY purchase_year_month, category_name
ORDER BY purchase_year_month, category_name
"""

# Executar a consulta usando a conexão existente
top_five_monthly = pd.read_sql_query(query_2, conn)
print(top_five_monthly.head())

# Criar gráfico de barras para as vendas mensais das 5 principais categorias de produtos
plt.figure(figsize=(12, 6))

# Definir cores diferentes para cada categoria
colors = ['#003366', '#00509E', '#0072B8', '#0095D9', '#00BFFF']  # Tons de azul

for idx, category in enumerate(top_five_monthly['category_name'].unique()):
    monthly_data = top_five_monthly[top_five_monthly['category_name'] == category]
    plt.bar(monthly_data['purchase_year_month'], monthly_data['monthly_sales'], label=category, color=colors[idx])

plt.title('Vendas Mensais das 5 Principais Categorias de Produtos', fontsize=16)
plt.xlabel('Mês e Ano', fontsize=14)
plt.ylabel('Total de Vendas (R$)', fontsize=14)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.gca().set_facecolor('lightgray')  # Fundo cinza claro
plt.legend(title='Categorias', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()  # Ajustar layout
plt.show()


# Encontrar a tendência de vendas mensais para o setor de ecommerce em geral
query_3 = """
SELECT
    strftime('%Y-%m', order_purchase_timestamp) AS purchase_year_month, -- usado para sqlite3
    SUM(i.price * i.order_item_id) AS monthly_sales
FROM ordens o
JOIN itens_pedidos i ON o.order_id = i.order_id
GROUP BY purchase_year_month
ORDER BY purchase_year_month
LIMIT 23
"""

# Executar a consulta usando a conexão existente
general_monthly = pd.read_sql_query(query_3, conn)
print(general_monthly)


# Gráfico 1: Categoria mais vendida em cada mês
# Primeiro, encontramos a categoria mais vendida de cada mês no resultado de top_five_monthly
top_five_monthly['category'] = top_five_monthly['category_name']  # Renomear a coluna para uso posterior

# Encontrar as categorias únicas e definir uma paleta de cores
unique_categories = top_five_monthly['category_name'].unique()
colors = plt.cm.get_cmap('Blues', len(unique_categories))

# Criar gráfico de categorias mais vendidas por mês
plt.figure(figsize=(12, 6))
for i, category in enumerate(unique_categories):
    monthly_sales = top_five_monthly[top_five_monthly['category_name'] == category]
    plt.bar(monthly_sales['purchase_year_month'], monthly_sales['monthly_sales'], 
            color=colors(i), label=category, width=0.4, align='center')

plt.title('Categoria Mais Vendida por Mês', fontsize=16)
plt.xlabel('Mês', fontsize=14)
plt.ylabel('Total de Vendas (R$)', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.gca().set_facecolor('lightgray')  # Fundo cinza claro
plt.legend(title='Categorias', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

# Gráfico 2: Totais de venda mês a mês
# Usar o resultado de general_monthly para o gráfico
plt.figure(figsize=(12, 6))
plt.bar(general_monthly['purchase_year_month'], general_monthly['monthly_sales'], color='navy')
plt.title('Total de Vendas Mês a Mês', fontsize=16)
plt.xlabel('Mês', fontsize=14)
plt.ylabel('Total de Vendas (R$)', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.gca().set_facecolor('lightgray')  # Fundo cinza claro
plt.tight_layout()
plt.show()

# Criar um dataframe df_region com as regiões para cada estado
df_region = pd.DataFrame({
    'sigla': ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'],
    'region_name': ['Norte', 'Nordeste', 'Norte', 'Norte', 'Nordeste', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Centro-Oeste', 'Nordeste', 'Centro-Oeste', 'Centro-Oeste', 'Sudeste', 'Norte', 'Nordeste', 'Sul', 'Nordeste', 'Nordeste', 'Sudeste', 'Sudeste', 'Sul', 'Norte', 'Norte', 'Sul', 'Sudeste', 'Sudeste', 'Centro-Oeste']
})
# Criação da tabela df_region no SQLite com os dados do DataFrame df_region
# Criar a tabela df_region diretamente a partir do DataFrame
df_region.to_sql('df_region', conn, if_exists='replace', index=False)

# Consultar informações de compra e localização (cidade, estado, região, coordenadas) para cada consumidor
query_4 = """
WITH geo AS (
    SELECT
        geolocation_zip_code_prefix,
        MIN(geolocation_lat) AS geolocation_lat,
        MIN(geolocation_lng) AS geolocation_lng
    FROM geolocalizacao
    GROUP BY geolocation_zip_code_prefix
)
SELECT DISTINCT
    o.customer_id,
    ROUND(SUM(i.price * i.order_item_id) OVER (PARTITION BY o.customer_id), 2) AS total_order_amount,
    COUNT(i.order_id) OVER (PARTITION BY o.customer_id) AS num_orders,
    ROUND(AVG(i.price * i.order_item_id) OVER (PARTITION BY o.customer_id), 2) AS avg_order_amount,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    CASE
        WHEN r.region_name = 'Norte' THEN 'North'
        WHEN r.region_name = 'Centro-Oeste' THEN 'Central-West'
        WHEN r.region_name = 'Nordeste' THEN 'Northeast'
        WHEN r.region_name = 'Sudeste' THEN 'Southeast'
        WHEN r.region_name = 'Sul' THEN 'South'
        ELSE r.region_name
    END AS region_name,
    geo.geolocation_lat,
    geo.geolocation_lng
FROM itens_pedidos i
LEFT JOIN ordens o ON o.order_id = i.order_id
LEFT JOIN consumidores c ON c.customer_id = o.customer_id
LEFT JOIN geo ON geo.geolocation_zip_code_prefix = c.customer_zip_code_prefix
LEFT JOIN df_region r ON c.customer_state = r.sigla
ORDER BY total_order_amount DESC
"""

avg_spending_location = pd.read_sql_query(query_4, conn)


print(avg_spending_location)





# Filtrar dados válidos, removendo NaNs e valores inválidos nas colunas de latitude e longitude
avg_spending_location_cleaned = avg_spending_location[
    avg_spending_location['geolocation_lat'].apply(lambda x: isinstance(x, (int, float)) and -90 <= x <= 90) &
    avg_spending_location['geolocation_lng'].apply(lambda x: isinstance(x, (int, float)) and -180 <= x <= 180)
]

# Criar um mapa centrado no Brasil
mapa_brasil = folium.Map(location=[-14.235, -51.9253], zoom_start=4, tiles='cartodbpositron')

# Adicionar um agrupamento de marcadores para melhorar a visualização
marker_cluster = MarkerCluster().add_to(mapa_brasil)

# Iterar sobre as linhas do dataframe avg_spending_location_cleaned e adicionar marcadores
for _, row in avg_spending_location_cleaned.iterrows():
    folium.Marker(
        location=[row['geolocation_lat'], row['geolocation_lng']],
        popup=f"City: {row['customer_city']}<br>"
              f"State: {row['customer_state']}<br>"
              f"Region: {row['region_name']}<br>"
              f"Total Order Amount: R$ {row['total_order_amount']}<br>"
              f"Number of Orders: {row['num_orders']}<br>"
              f"Avg Order Amount: R$ {row['avg_order_amount']}",
        icon=folium.Icon(color='blue')
    ).add_to(marker_cluster)

# Salvar o mapa em um arquivo HTML
mapa_brasil.save(r'C:\Users\User\Documents\portfolio\mapa_pedidos_brasil.html')



# Query para obter as vendas mensais por região
query_5 = """
WITH geo AS (
    SELECT
        geolocation_zip_code_prefix,
        MIN(geolocation_lat) AS geolocation_lat,
        MIN(geolocation_lng) AS geolocation_lng
    FROM geolocalizacao
    WHERE 
        geolocation_lat <= 5.27438888
        AND geolocation_lng >= -73.98283055
        AND geolocation_lat >= -33.75116944
        AND geolocation_lng <= -34.79314722
    GROUP BY geolocation_zip_code_prefix
)
SELECT
    CASE
        WHEN r.region_name = 'Norte' THEN 'North'
        WHEN r.region_name = 'Centro-Oeste' THEN 'Central-West'
        WHEN r.region_name = 'Nordeste' THEN 'Northeast'
        WHEN r.region_name = 'Sudeste' THEN 'Southeast'
        WHEN r.region_name = 'Sul' THEN 'South'
        ELSE r.region_name
    END AS region_name,
    strftime('%Y-%m', o.order_purchase_timestamp) AS purchase_year_month,
    SUM(i.price * i.order_item_id) AS total_sales,
    COUNT(i.order_id) AS total_orders
FROM itens_pedidos i
LEFT JOIN ordens o ON o.order_id = i.order_id
LEFT JOIN consumidores c ON c.customer_id = o.customer_id
LEFT JOIN geolocalizacao geo ON geo.geolocation_zip_code_prefix = c.customer_zip_code_prefix
LEFT JOIN df_region r ON r.sigla = c.customer_state
WHERE purchase_year_month <= '2018-08' AND purchase_year_month >= '2017-01'
GROUP BY region_name, purchase_year_month
ORDER BY region_name, purchase_year_month
"""

# Executar a query e obter os resultados
monthly_sales_region = pd.read_sql_query(query_5, conn)

# Ver os primeiros resultados
monthly_sales_region.head()

# Transformar a coluna purchase_year_month para formato de data (caso necessário)
monthly_sales_region['purchase_year_month'] = pd.to_datetime(monthly_sales_region['purchase_year_month'], format='%Y-%m')

# Criar o gráfico de vendas mensais por região
plt.figure(figsize=(10, 6))

# Traçar a série temporal para cada região
regions = monthly_sales_region['region_name'].unique()
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']  # Tons de cores diferentes para cada região

for i, region in enumerate(regions):
    region_data = monthly_sales_region[monthly_sales_region['region_name'] == region]
    plt.plot(region_data['purchase_year_month'], region_data['total_sales'], label=region, color=colors[i])

# Personalizar o gráfico
plt.title('Vendas Mensais por Região', fontsize=16)
plt.xlabel('Mês', fontsize=14)
plt.ylabel('Total de Vendas (R$)', fontsize=14)
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)
plt.gca().set_facecolor('lightgray')  # Fundo cinza claro

# Adicionar a legenda
plt.legend(title='Região')

# Mostrar o gráfico
plt.tight_layout()
plt.show()
# Criar a tabela avg_spending_location no banco de dados SQLite a partir do DataFrame
avg_spending_location.to_sql('avg_spending_location', conn, if_exists='replace', index=False)

# Query ajustada para obter vendas totais e valor médio por consumidor por estado
query_6 = """
SELECT
    customer_state AS state,
    SUM(total_order_amount) AS total_sales,
    AVG(total_order_amount) AS avg_lifetime_value
FROM avg_spending_location
GROUP BY state
ORDER BY total_sales DESC
"""

# Executar a query e obter os resultados
state_spending = pd.read_sql_query(query_6, conn)

# Ver os primeiros resultados
state_spending.head()

# Calcular o estado e os valores a serem plotados
state_spending = state_spending.sort_values(by='total_sales', ascending=False)

# Criar o gráfico de pirâmide etária
fig, ax1 = plt.subplots(figsize=(10, 8))

# Configurar o gráfico para total de vendas (esquerda)
sns.barplot(x='total_sales', y='state', data=state_spending, ax=ax1, color="teal", label='Total de Vendas')

# Configurar o gráfico para valor médio por consumidor (direita)
ax2 = ax1.twiny()
sns.barplot(x='avg_lifetime_value', y='state', data=state_spending, ax=ax2, color="lightseagreen", label='Valor Médio por Consumidor')

# Configuração dos eixos
ax1.set_xlabel('Total de Vendas', fontsize=14)
ax2.set_xlabel('Valor Médio por Consumidor', fontsize=14)

# Adicionando título
plt.title('Total de Vendas e Valor Médio por Consumidor por Estado', fontsize=16)

# Ajustar os limites do eixo X do primeiro gráfico (esquerda)
ax1.set_xlim(0, state_spending['total_sales'].max() * 1.1)

# Ajustar os limites do eixo X do segundo gráfico (direita)
ax2.set_xlim(0, state_spending['avg_lifetime_value'].max() * 1.1)

# Desativar a grade
ax1.grid(False)
ax2.grid(False)

# Ajustar os ticks do eixo Y
ax1.set_yticks(range(len(state_spending)))
ax1.set_yticklabels(state_spending['state'])

# Mostrar gráfico
plt.tight_layout()
plt.show()

# Query para contar o total de ordens por mês
query_7 = """
SELECT
    strftime('%Y-%m', order_purchase_timestamp) AS purchase_year_month, -- Extrair Ano-Mês
    COUNT(order_id) AS total_year_monthly_orders
FROM ordens
WHERE purchase_year_month > '2016-12' AND purchase_year_month < '2018-09'
GROUP BY purchase_year_month
ORDER BY purchase_year_month
"""
year_monthly_orders = pd.read_sql_query(query_7, conn)

# Criar gráfico das ordens totais por mês
plt.figure(figsize=(12, 6))
sns.barplot(x='purchase_year_month', y='total_year_monthly_orders', data=year_monthly_orders, color='blue')
plt.title('Total de Ordens por Mês', fontsize=16)
plt.xlabel('Ano-Mês', fontsize=14)
plt.ylabel('Total de Ordens', fontsize=14)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Query para contar ordens mensais em 2017 e 2018
query_8 = """
SELECT
    strftime('%m', order_purchase_timestamp) AS purchase_month,         -- Extrair Mês
    COUNT(order_id) FILTER(WHERE strftime('%Y', order_purchase_timestamp)='2017') AS monthly_orders_2017,
    COUNT(order_id) FILTER(WHERE strftime('%Y', order_purchase_timestamp)='2018') AS monthly_orders_2018
FROM ordens
GROUP BY purchase_month
ORDER BY purchase_month
"""
monthly_orders = pd.read_sql_query(query_8, conn)

# Criar gráfico de diferença de ordens entre 2017 e 2018
plt.figure(figsize=(12, 6))
bar_width = 0.35
x = np.arange(len(monthly_orders['purchase_month']))

# Barras para 2017 e 2018
plt.bar(x - bar_width/2, monthly_orders['monthly_orders_2017'], width=bar_width, label='2017', color='blue')
plt.bar(x + bar_width/2, monthly_orders['monthly_orders_2018'], width=bar_width, label='2018', color='orange')

plt.title('Diferença de Ordens Mensais entre 2017 e 2018', fontsize=16)
plt.xlabel('Mês', fontsize=14)
plt.ylabel('Total de Ordens', fontsize=14)
plt.xticks(x, monthly_orders['purchase_month'], rotation=0)
plt.legend()
plt.tight_layout()
plt.show()


# Query para listar ordens com dia da semana e hora da compra
query_9 = """
SELECT
    order_id,
    strftime('%w', order_purchase_timestamp) AS purchase_day_of_week,   -- Extrair Dia da Semana (0 = Domingo, 6 = Sábado)
    strftime('%H', order_purchase_timestamp) AS purchase_hour          -- Extrair Hora
FROM ordens
"""
orders_day_hour = pd.read_sql_query(query_9, conn)

# Criar gráfico de densidade de compras por hora do dia
plt.figure(figsize=(12, 6))
sns.kdeplot(data=orders_day_hour, x='purchase_hour', fill=True, bw_adjust=0.5)
plt.title('Densidade de Compras por Hora do Dia', fontsize=16)
plt.xlabel('Hora do Dia', fontsize=14)
plt.ylabel('Densidade', fontsize=14)
plt.xticks(range(0, 24))
plt.grid()
plt.tight_layout()
plt.show()

# Query para calcular vendas totais por vendedor e sua participação no mercado
query_10 = """
SELECT DISTINCT
    i.seller_id,
    SUM(i.price * i.order_item_id) OVER (PARTITION BY i.seller_id) AS sales,
    SUM(i.order_item_id) OVER (PARTITION BY i.seller_id) AS order_volume,
    (SUM(i.price * i.order_item_id) OVER (PARTITION BY i.seller_id) * 100.0 / 
        (SELECT SUM(price * order_item_id) FROM itens_pedidos WHERE seller_id IS NOT NULL)) AS market_share
FROM itens_pedidos i
WHERE i.seller_id IS NOT NULL
ORDER BY sales DESC
"""
general_market_share = pd.read_sql_query(query_10, conn)

# Criar gráfico de distribuição entre total de ordens e total de vendas
plt.figure(figsize=(12, 6))
sns.scatterplot(data=general_market_share, x='order_volume', y='sales', hue='market_share', size='market_share', sizes=(20, 200), alpha=0.6, palette='viridis')
plt.title('Distribuição entre Total de Ordens e Total de Vendas por Vendedor', fontsize=16)
plt.xlabel('Total de Ordens', fontsize=14)
plt.ylabel('Total de Vendas', fontsize=14)
plt.grid()
plt.tight_layout()
plt.show()





# Confirmar e fechar a conexão
conn.commit()
conn.close()

