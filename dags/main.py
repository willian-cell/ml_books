from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2025, 5, 8), # data de hoje
     'retries': 1,
     'retry_delay': timedelta(minutes=5),
     'schedule_interval': '@daily'
}

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": '"Not(A:Brand";v="99", "Opera GX";v="118", "Chromium";v="133"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "Windows",
     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0',
}

dag = DAG(
    'books_main',
    default_args=default_args,
    description='Primeira DAG'
)


# TAREFAS 01
def iniciar_pipeline(ti):
     print(">>Debug: Pipeline e primeira tarefa configurada com sucesso!")

init_task = PythonOperator(
     task_id='init',
     python_callable=iniciar_pipeline,
     dag=dag,
)

# TAREFA 02 - > Obter os dados do mercado livre
def get_books(num_books, ti): # -> função python para executar a segunda task (obter dados)
     base_url = f"https://lista.mercadolivre.com.br/data-engineering-books" # -> url do ml
     books = [] # -> array que guarda os livros obtidos, com chave e valor
     seen_titles = set() # -> array apenas de títulos, para verificar se tem repetido
     first_item_page = 1 # -> página inicial, será incrementado ao fim de cada loop
     
     while len(books) < num_books: # -> loop para executar a cada página
          url = f"{base_url}_NoIndex_True" # -> url para primeira página
          if first_item_page > 1:
               url = f"{base_url}_Desde_{first_item_page}_NoIndex_True" # -> url para segunda página em diante
          print(f">>Debug: url a acessar: {url}")
          response = requests.get(url, headers=headers) # -> obter requisição get http passando os cabeçalhos
          if response.status_code == 200: # -> se a requisição funcionou...
               soup = BeautifulSoup(response.content, "html.parser") # -> formata o html utilizando a lib BeautifulSoup
               book_containers = soup.find_all("li", {"class": "ui-search-layout__item"}) # -> obtem o elemento que contém o item
               for book in book_containers: # -> para cada item recebido...
                    title = book.find("a", {"class": "poly-component__title"}) # -> obter o elemento html que contém o título
                    price = book.find("span", {"class": "andes-money-amount__fraction"}) # -> obter o elemento html que contém o preço
                    if title and price: # -> se obter com sucesso um título e um preço
                         book_title = title.text.strip() # -> remove espaços e formata o título de html para texto
                         if book_title not in seen_titles: # -> verifica se o título do livro já não foi adicionado
                              seen_titles.add(book_title) # -> adiciona os não repetidos em um array para verificar no próximo loop
                              books.append({ # -> salva o título e preço em um array com chave e valor: {{title: livroA, price: 200},{title: livroA, price: 200}}
                                   "Title": book_title,
                                   "Price": price.text.strip()
                              })
               first_item_page += 50 # -> incrementa a página para no próximo loop pegar os itens da próxima página
               if first_item_page > 200: # -> evitar loop infinito
                    break
          else: # -> mostrar erro caso a requisição falhe
               print(response.status_code)
               print(response.text)
               break
          
     books = books[:num_books] # -> list slicing: se o array for maior do que queremos, corte o restante
     print(f">>Debug: qtd.books: {len(books)}") # -> mostra no terminal a quantidade de livros obtidos (100 ou menos)
     df = pd.DataFrame(books)
     df.drop_duplicates(subset="Title", inplace=True)
     ti.xcom_push(key='book_data', value=df.to_dict('records'))

fetch_book_data_task = PythonOperator(
     task_id='fetch_book_data',
     python_callable=get_books,
     op_args=[100],
     dag=dag,
)

# TAREFA 03 -> criar tabela
create_table_task = SQLExecuteQueryOperator(
     task_id='create_table',
     conn_id='books_connection',
     sql="""
     CREATE TABLE IF NOT EXISTS books_ml (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          price TEXT)
     """,
     dag=dag,
)

# TAREFA 04
def insert_book_data_into_postgres(ti):
     book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
     if not book_data:
          raise ValueError("No book data found")
     postgres_hook = PostgresHook(postgres_conn_id='books_connection')
     insert_query = """
     INSERT INTO books_ml (title, price)
     VALUES (%s, %s)
     """
     for book in book_data:
          postgres_hook.run(insert_query, parameters=(book['Title'], book['Price']))

insert_book_data_task = PythonOperator(
     task_id='insert_book_data',
     python_callable=insert_book_data_into_postgres,
     dag=dag,
)

# DEPENDENCIAS
init_task >> fetch_book_data_task >> create_table_task >> insert_book_data_task
