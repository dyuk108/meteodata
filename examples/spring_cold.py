# -*- coding: utf-8 -*-
#
# Запросы к БД по нахождению дат последних весенних заморозков.
# Является частью проекта "Погода для садовода" https://pogoda.dyuk108.ru
#
# Дмитрий Клыков, 2025. dyuk108.ru

import duckdb

path_db = '/YandexDisk/_data/' # путь к БД (следует установить свой)
beg_year = 2015 # первый год в последовательности
end_year = 2024 # последний год в последовательности
station = 27625 # для какой станции выводим информацию

# Подключаемся к файлу базы данных (создастся, если не существует)
con = duckdb.connect(f'{path_db}meteodata.db')

# Запрос: для каждого года находится последняя дата, 
# когда минимальная температура опускалась до 0 или ниже градусов.
query = f"""WITH years AS (
SELECT UNNEST(GENERATE_SERIES({beg_year}, {end_year})) as year
)
SELECT 
    (SELECT MAX(date::DATE) 
     FROM synoptic 
     WHERE station_id = {station}
       AND EXTRACT(YEAR FROM date) = y.year
       AND temperature <= 0
       AND date <= MAKE_DATE(y.year, 7, 1)) as last_cold_date
FROM years y
ORDER BY y.year;
"""

df = con.execute(query).df()
print(df)

con.close()