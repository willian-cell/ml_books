from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup

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
     print("Hello, World!")

init_task = PythonOperator(
     task_id='init',
     python_callable=iniciar_pipeline,
     dag=dag,
)

# TAREFA 02
def get_books(num_books, ti):
     print("Task get_books")
     base_url = f"https://lista.mercadolivre.com.br/data-engineering-books"
     books = []
     seen_titles = set()
     first_item_page = 1
     
     #while len(books) < num_books:
     url = f"{base_url}_Desde_{first_item_page}_NoIndex_True"
     print("url:")
     print(url)
     response = requests.get(url, headers=headers)
     print("resposta...............")
     print(response.status_code)
     if response.status_code == 200:
          soup = BeautifulSoup(response.content, "html.parser")
          book_containers = soup.find_all("li", {"class": "ui-search-layout__item"})
          for book in book_containers:
               title = book.find("a", {"class": "poly-component__title"})
               price = book.find("span", {"class": "andes-money-amount__fraction"})
               if title and price:
                    book_title = title.text.strip()
                    if book_title not in seen_titles:
                         seen_titles.add(book_title)
                         books.append({
                              "Title": book_title,
                              "Price": price.text.strip()
                         })
          first_item_page += 50
     else:
          print(response.status_code)
          print(response.text)

     size = len(books)
     print("tamanho books")
     print(size)

fetch_book_data_task = PythonOperator(
     task_id='fetch_book_data',
     python_callable=get_books,
     op_args=[100],
     dag=dag,
)

# DEPENDENCIAS
init_task >> fetch_book_data_task