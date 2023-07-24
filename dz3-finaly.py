from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import asyncio
from aiohttp import ClientSession
import requests
import time
import json
import wget 
from pathlib import Path
import logging
import zipfile
import sqlite3
from collections import Counter


default_args = {
    'owner': 'allder',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Настройка логера
logger = logging.getLogger(__name__)

#Переменные
url = 'https://ofdata.ru/open-data/download/egrul.json.zip' 
file_zip = 'egrul.json.zip'
file_path = Path(Path.cwd(), file_zip)
company_list = []
sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

itog_list_api = []

# Функция для скачивания zip архива
def download_zfile():
    try:
        wget.download(url, str(file_path.absolute()))
        logger.info(f"Завершена загрузка файла с адреса {url}")
    except:
        logger.info(f"Ошибка загрузки файла с адреса {url}")

# Функция для поиска компаний с ОКВЕД=61
def parsing():
# проверяем существует ли архив с файлами
    if Path(file_path).exists():
# Открываем zip архив как файл
        with zipfile.ZipFile (file_zip, 'r') as zipobj:
# Получаем список файлов в zip архиве
            file_names = zipobj.namelist()
# Запускаем цикл по именам файлов в zip архиве
            for name in file_names:
# Открываем один файл в zip архиве
                with zipobj.open(name) as file:
# Читаем один файл в zip архиве
                    json_data = file.read()
#Парсим файл JSON и ищем в нем организации с основным ОКВЭД 61
                    data = json.loads(json_data) 
                    for i in data:
                        if 'inn' in i.keys():
                            if 'СвОКВЭД' in i['data'].keys():
                                if 'СвОКВЭДОсн' in i['data']['СвОКВЭД'].keys():
                                    if (i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2]) == '61':
                                        data_tuple = (i['ogrn'], i['inn'], i['name'], i['full_name'], i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'])
                                        company_list.append(data_tuple) 
                logger.info(f"Завершена обработка файла {name}")
                print(name)                        
    else:
        logger.info(f" Файл не найден: {file_zip}")
#Создание таблицы в БД и запись данных о команиях с ОКВЭД 61 
    try:
# создание таблицы telecom_companies
        create_names_table = """
        CREATE TABLE IF NOT EXISTS telecom_companies(
            ogrn INTEGER PRIMARY KEY NOT NULL,    
            inn INTEGER,
            name TEXT,
            full_name TEXT,
            okved TEXT 
        )
        """
        sqlite_hook.run(create_names_table)
        logger.info("Создана Таблица telecom_companies")
# загрузка данных
        fields = ['ogrn', 'inn', 'name', 'full_name', 'okved']
        sqlite_hook.insert_rows(
               table = 'telecom_companies',
               rows = company_list,
               target_fields = fields,
        )
        logger.info("Данные загружены в таблицу telecom_companies")
    except sqlite3.Error as error:
        logger.info(f"Ошибка при работк с БД {error}")

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
            logger.info("Ошибка при получении данных о вакансии") 
            break   
    return vacancies_list_api

# Функция для получения вакансий от компаний с ОКВЕД=61 с сайта hh.ru
def parsing_site():
# получение из БД спсика наименований компаний с ОКВЕД=61    
    try:
        print('Start')
        sql_select = 'select name from telecom_companies'
        company61 = sqlite_hook.get_records(sql_select)
        logger.info('Загружены данные о компаниях')
    except:
        logger.info("Ошибка при работк с БД")
# просматриваем все вакансии и отбираем компании, которые попали в спсиок ОКВЕД 61, просматриваем пока в ответе есть вакансии
    kk = 0
    page = 0
    while kk == 0:
# параметры для запроса GET к сайту hh.ru
        params = {'text': 'python middle developer',
                  'search_field': 'name',
                  'page': page,
                  'per_page': 30 }
        url_satrt = 'https://api.hh.ru/vacancies'
        user_agent = {'User-agent': 'Mozilla/5.0'}
# запрос к сайту 
        req = requests.get(url_satrt, params, headers=user_agent)
        req.close()
        logger.info(f" статус ответа {req.status_code}")
# проверка статуса ответа, если ОК получение спсика вакансий
        if req.status_code == 200:
            vacancies = req.json().get('items')
            if len(vacancies) != 0:
                page +=1    
                url_list = []
                for vacancy in vacancies:
                    strv = vacancy['employer']['name'].upper()
# отсеивание вакансий от компаний с ОКВЕД=61, путем проверки вхождения названия комании с вакансии в список имен компаний с ОКВЕД=61                    
                    for i in company61:
                        if i[0].find(strv) != -1:
                            url_list.append(vacancy['url'][28:36])
                            break
            else:
                logger.info("Вакансии закончились")
                kk = 1
# получения спсика требуемых параметров о вакансиях
#            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # команда для работы асинхронного режима в виндовс
            temp_list_api = asyncio.run(main(url_list))   
            itog_list_api.extend(temp_list_api) 
        time.sleep(5) # чтобы не превысить допустимое количество запросов в секунду      
#Создание таблицы в БД и запись данных о вакансиях команий с ОКВЭД 61 
    try:
# создание таблицы telecom_companies
        create_names_table = """
        CREATE TABLE IF NOT EXISTS vacancies(
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            company_name TEXT,
            position TEXT,
            job_description TEXT,
            key_skills TEXT
        )
        """
        sqlite_hook.run(create_names_table)
        logger.info("Создана Таблица vacancies")
# загрузка данных
        fields = ['company_name', 'position', 'job_description', 'key_skills']
        sqlite_hook.insert_rows(
               table = 'vacancies',
               rows = itog_list_api,
               target_fields = fields,
        )
        logger.info("Данные загружены в таблицу vacancies")
    except:
        logger.info("Ошибка при работe с БД")

# Поиск топ10 ключевых навыков в вакансиях команий с ОКВЕД=61
def top_keyskills():
    skill_list = []
# получение из БД спсика ключевых навыков    
    try:
        print('Start')
        sql_select = 'select key_skills from vacancies'
        sql_temp = sqlite_hook.get_records(sql_select)
        logger.info('Загружены данные о ключевых навыках')
    except:
        logger.info("Ошибка при работк с БД")
    for row in sql_temp:
        templist = row[0].split(',') # преобразуем строку с ключевыми навыками в список
# создаем общий спсиок ключевых навыков по найденным вакансиям        
        for skil in templist:
            if len(skil)!=0:
                skill_list.append(skil)
    skills = Counter(skill_list)
# Вывод 10-ти самых востребованных навыков из Вакансий команий с ОКВЕД=61 на сайте hh.ru    
    print(skills.most_common(10))            

    print('END')

with DAG(
    dag_id='dz3_finaly',
    default_args=default_args,
    description='DAG for donwload and parsing',
    start_date=datetime(2023, 7, 20, 8),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='download_zipfile',
        python_callable=download_zfile,
    )
    task2 = PythonOperator(
        task_id='parsing_zipfile',
        python_callable=parsing,
    )
    task3 = PythonOperator(
        task_id='parsing_hh',
        python_callable=parsing_site,
    )
    task4 = PythonOperator(
        task_id='Top_key_skills',
        python_callable=top_keyskills,
    )

    task1 >> task2 >> task3 >> task4