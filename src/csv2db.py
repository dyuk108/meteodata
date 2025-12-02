# -*- coding: utf-8 -*-
#
# Скрипт читает срочные метеоданные с добавленными данными о снежном покрове
# (файлы CSV) в файл базы данных DuckDB.
# Является частью проекта "Погода для садовода" https://pogoda.dyuk108.ru
#
# Дмитрий Клыков, 2025. dyuk108.ru

import duckdb

path_csv = '/catalogs/Srok8c/Srok8c_csv/' # откуда брать CSV-файлы
path_csv2 = 'data/'
path_out = '/catalogs/' # куда записывать результат

import duckdb

# Подключаемся к файлу базы данных (создастся, если не существует)
con = duckdb.connect(f'{path_out}meteodata.db')

# 1. Загрузка meteodata из нескольких CSV
meteodata_schema = {
    'date': 'TIMESTAMP', # дата, время
    'station_id': 'INTEGER', # индекс станции
    'weather_code': 'INTEGER', # код погоды между сроками
    'wind_direction': 'INTEGER', # направление ветра
    'wind_speed': 'INTEGER', # средняя скорость ветра, м/с
    'rain': 'FLOAT', # сумма осадков, мм
    'ground_temperature': 'FLOAT', # температура поверхности почвы
    'temperature': 'FLOAT', # температура воздуха по сухому термометру
    'humidity': 'INTEGER', # относительная влажность воздуха
    'pressure': 'INTEGER', # атмосферное давление на уровне станции
    'snow': 'INTEGER' # уровень снежного покрова (см, если есть)
}

con.execute(f"""
    CREATE TABLE meteodata AS
    SELECT * FROM read_csv('{path_csv}*.csv',
                           header=true,
                           columns=?,
                           auto_detect=false)
""", [meteodata_schema])

# 2. Загрузка meteostations из одного файла
stations_schema = {
    'station_id': 'INTEGER', # индекс станции
    'name': 'VARCHAR', # название станции
    'lat': 'FLOAT', # широта
    'lon': 'FLOAT', # долгота
    'h': 'INTEGER', # высота над уровнем моря
    'province_id': 'INTEGER' # ID региона
}

con.execute(f"""
    CREATE TABLE stations AS
    SELECT * FROM read_csv('{path_csv2}meteostations.csv',
                           header=true,
                           columns=?,
                           auto_detect=false)
""", [stations_schema])

# 3. Загрузка provinces
provinces_schema = {
    'province_id': 'INTEGER',
    'name': 'VARCHAR'
}

con.execute(f"""
    CREATE TABLE provinces AS
    SELECT * FROM read_csv('{path_csv2}provinces.csv',
                           header=true,
                           columns=?,
                           auto_detect=false)
""", [provinces_schema])

# (Опционально) Проверка
print("meteodata count:", con.execute("SELECT COUNT(*) FROM meteodata").fetchone()[0])
print("stations count:", con.execute("SELECT COUNT(*) FROM stations").fetchone()[0])
print("provinces count:", con.execute("SELECT COUNT(*) FROM provinces").fetchone()[0])

con.close()