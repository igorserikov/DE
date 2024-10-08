## Подключаемся

clickhouse-client --password

show databases;
----------
Graphite — это система для сбора и визуализации метрик.
Carbon — компонент, который получает и сохраняет метрики.
Whisper — база данных с таймсериями.
Graphite Web — веб-интерфейс
----------
Проект graphouse - https://github.com/ClickHouse/graphouse

CREATE DATABASE graphite;

drop database graphite;

## Создаем табличку с данными:

CREATE TABLE graphite.data ( 
metric String, 
value Float64, 
timestamp UInt32, 
date Date, 
updated UInt32 ) 
ENGINE = ReplacingMergeTree(updated) 
PARTITION BY toMonday(date) 
ORDER BY (metric, timestamp);

> use graphite
> show tables;
----------
Использование updated: 
Поскольку  ReplacingMergeTree, это позволяет ClickHouse автоматически 
заменять более старые записи новыми при наличии одинаковых значений 
ключа сортировки (в вашем случае — это будет metric и timestamp).
-----------
Далее делаем 
> OPTIMIZE TABLE graphite.data;
> SELECT * FROM graphite.data;
