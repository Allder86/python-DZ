# импорт модулей
import json
import sqlite3

filename = 'D:\DZ\okved_2.json'

# попытка чтения файла в список templates, если файл не находитcя, то возвращается пустой список
try:
    with open(filename, 'r', encoding='utf-8') as f:
        templates = json.load(f)
except FileNotFoundError:
    print(f"Запрашиваемый файл {filename } не найден") 
    templates = []
    
# попытка коннекта к БД, загрузка данных в БД   
try:
    sqlite_connection = sqlite3.connect('C:\sqlite\hw1.db')
    cursor = sqlite_connection.cursor()
    print("Подключен к SQLite")
    for i in templates:
        sqlite_insert_query = "INSERT INTO okved (code, parent_code, section, name, comment) VALUES (?, ?, ?, ?, ?);"
        data_tuple = (i['code'], i['parent_code'], i['section'], i['name'], i['comment'])
        cursor.execute(sqlite_insert_query, data_tuple)
        sqlite_connection.commit()

    cursor.close()
    sqlite_connection.close()

except sqlite3.Error as error:
    print("Ошибка при работе с SQLite", error)
