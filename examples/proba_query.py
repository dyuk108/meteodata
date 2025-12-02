# -*- coding: utf-8 -*-
#
# Запросы к БД, проба.
# Является частью проекта "Погода для садовода" https://pogoda.dyuk108.ru
#
# Дмитрий Клыков, 2025. dyuk108.ru

import duckdb

path_db = '/catalogs/' # путь к БД

import duckdb

# Подключаемся к файлу базы данных (создастся, если не существует)
con = duckdb.connect(f'{path_db}meteodata.db')

# Список таблиц
res = con.execute("SHOW TABLES").fetchdf()
print(res)
print('-----------------')

# Список регионов
res = con.execute("SELECT * FROM provinces").fetchdf()
print(res)
print('-----------------')

# Список метеостанций с названиями регионов
res = con.execute("""SELECT stations.station_id, stations.name, provinces.name AS province
FROM stations
JOIN provinces ON provinces.province_id = stations.province_id""").fetchdf()
print(res)
print('-----------------')


con.close()