# импорт модулей
import json
import sqlite3
from pathlib import Path

#попытка коннекта к БД
try:
    sqlite_connection = sqlite3.connect('C:\sqlite\hw1.db')
    cursor = sqlite_connection.cursor()

    entries = Path('D:\DZ\egrul.json')

# читаем каталог с файлами, если он существует
    if Path(entries).exists():
        for entry in entries.iterdir():
            print(entry.name)
            p = Path(entries, entry.name)
# читаем каждый файл по отдельности и ищем в нем организации с основным ОКВЭД 61 и загружаем в БД       
            with open(p, 'r', encoding='utf-8') as f:
                templates = json.load(f)
            for i in templates:
                if 'inn' in i.keys():
                    if 'СвОКВЭД' in i['data'].keys():
                        if 'СвОКВЭДОсн' in i['data']['СвОКВЭД'].keys():
                            if (i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2]) == '61':
                                sqlite_insert_query = "INSERT INTO telecom_companies (ogrn, inn, name, full_name, okved) VALUES (?, ?, ?, ?, ?);"
                                data_tuple = (i['ogrn'], i['inn'], i['name'], i['full_name'], i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'])
                                cursor.execute(sqlite_insert_query, data_tuple)
                                sqlite_connection.commit()
                                print(data_tuple)
    else:
        print('Каталог', entries, 'не найден')

    cursor.close()
    sqlite_connection.close()

except sqlite3.Error as error:
    print("Ошибка при работе с SQLite", error)
    
# p.s. команды вывода добавлены, чтобы отсеживать ход выполнения программы
