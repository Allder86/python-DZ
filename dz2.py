# импорт модулей
import json
import requests
from bs4 import BeautifulSoup
import time
import asyncio
from aiohttp import ClientSession
import sqlite3
from pathlib import Path

bd = 'C:\sqlite\hw1.db'

# 1й способ
# парсинг веб страниц

# параметры для запроса GET к сайту hh.ru
params = {'text': 'python middle developer',
          'search_field': 'name',
          'area': 1,
          'page': 0,
          'per_page': 100 }
url = 'https://hh.ru/search/vacancy'
user_agent = {'User-agent': 'Mozilla/5.0'}

# запрос к сайту 
req = requests.get(url, params, headers=user_agent)
# проверка статуса ответа, если ОК получение спсика вакансий
if req.status_code == 200:
    soup = BeautifulSoup(req.content, 'lxml')
    vacans_links = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
    vacancies_list = []
# перебор вакансий по полученному спсику (ссылки на вакансии) и получение спсика результатов с запрашиваемыми полями (Название компании, Название позиции (должности), Описание вакансии, Список ключевых навыков)
    for link in vacans_links:
        url2=link.attrs.get('href')
        req_link = requests.get(url2, headers=user_agent)
        vacancy_info = []
# проверка статуса ответа, если ОК получение данных о вакансии и добавление их в список       
        if req_link.status_code == 200:
            soup2 = BeautifulSoup(req_link.content, 'lxml')
            position = soup2.find('h1')
#            company_name = soup2.find('a', attrs={'data-qa': 'vacancy-company-name'})
            company_name = soup2.find('span', attrs={'class': 'vacancy-company-name'})
            job_description = soup2.find('div', attrs={'data-qa': 'vacancy-description'})
            skills = soup2.find_all('span', attrs={'data-qa': 'bloko-tag__text'})
            str_skills = ''
            list_skills = []
            if skills:
                for skil in skills:
                    list_skills.append(skil.text)
                str_skills = ','.join(list_skills)  
            data = (company_name.text, position.text, job_description.text, str_skills)
            vacancies_list.append(data)
        else:
            print('Ошибка получения данных со старницы вакансии:', url2)   

for i in vacancies_list:
    print(i[0])

# создание таблицы vacancies1 и запись полученных данных в БД
try:
    sqlite_connection = sqlite3.connect(bd)
    cursor = sqlite_connection.cursor()   
# создание таблицы vacancies1
    create_names_table = """
    CREATE TABLE IF NOT EXISTS vacancies1(
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,    
        company_name TEXT,
        position TEXT,
        job_description TEXT,
        key_skills TEXT 
   )
   """
    cursor.execute(create_names_table)
    sqlite_connection.commit() 
# загрузка данных
    insert_several_rows_parameters = """
    INSERT INTO vacancies1 (company_name, position, job_description, key_skills)
    VALUES (?, ?, ?, ?)
    """
    cursor.executemany(insert_several_rows_parameters, vacancies_list)
    sqlite_connection.commit()
    cursor.close()
    sqlite_connection.close()
except sqlite3.Error as error:
    print("Ошибка при работе с SQLite", error)


# 2й способ
# API

itog_list_api =[] 

# функция получения данных о вакансии
async def get_vacancy(id, session):
    url = f'/vacancies/{id}'
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        return vacancy_json
    
# функция асинхронного получения данных о вакансиях по списку url и обработки полученных результатов, возвращает список требуемых параметров
async def main(ids):
    async with ClientSession('https://api.hh.ru/') as session:
        tasks = []
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))
        results = await asyncio.gather(*tasks)
    vacancies_list_api = []
    for result in results:
        str_skills_api = ''
        list_skills_api = []
        if 'errors' not in result.keys():
            if result['key_skills']:
                for skil in result['key_skills']:
                    list_skills_api.append(skil['name'])
                str_skills_api = ','.join(list_skills_api)
            data = (result['employer']['name'], result['name'], result['description'], str_skills_api)   
            vacancies_list_api.append(data)
        else:
            print('Ошибка получение данных о вакансии') 
            break   
    return vacancies_list_api

# разбиваем 100 вакансий на 5 страниц по 20 для уменьшения числа запросов
for page in range(0,5):
    # параметры для запроса GET к сайту hh.ru
    params = {'text': 'python middle developer',
              'search_field': 'name',
              'page': page,
              'per_page': 20 }
    url_satrt = 'https://api.hh.ru/vacancies'
    user_agent = {'User-agent': 'Mozilla/5.0'}
# запрос к сайту 
    req = requests.get(url_satrt, params, headers=user_agent)
    req.close()
# проверка статуса ответа, если ОК получение спсика вакансий
    if req.status_code == 200:
        vacancies = req.json().get('items')
        url_list = []
        for vacancy in vacancies:
            url_list.append(vacancy['url'][28:36])

# получения спсика требуемых параметров о вакансиях
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        temp_list_api = asyncio.run(main(url_list))   
        itog_list_api.extend(temp_list_api) 
    time.sleep(1) # чтобы не превысить допустимое количество запросов в секунду

# создание таблицы vacancies2 и запись полученных данных в БД
try:
    sqlite_connection = sqlite3.connect(bd)
    cursor = sqlite_connection.cursor()   
# создание таблицы vacancies2
    create_names_table = """
    CREATE TABLE IF NOT EXISTS vacancies2(
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,    
        company_name TEXT,
        position TEXT,
        job_description TEXT,
        key_skills TEXT 
   )
   """
    cursor.execute(create_names_table)
    sqlite_connection.commit() 
# загрузка данных
    insert_several_rows_parameters = """
    INSERT INTO vacancies2 (company_name, position, job_description, key_skills)
    VALUES (?, ?, ?, ?)
    """
    cursor.executemany(insert_several_rows_parameters, itog_list_api)
    sqlite_connection.commit()
    cursor.close()
    sqlite_connection.close()
except sqlite3.Error as error:
    print("Ошибка при работе с SQLite", error)
