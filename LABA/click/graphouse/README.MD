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

Использование updated: 
Поскольку  ReplacingMergeTree, это позволяет ClickHouse автоматически 
заменять более старые записи новыми при наличии одинаковых значений 
ключа сортировки (в вашем случае — это будет metric и timestamp).

Вставляем записи с updated=1:

INSERT INTO graphite.data (metric, value, timestamp, date, updated) VALUES
 ('servers.host1.cpu.usage', 70.0, 1633035600, '2021-09-30', 1), ('servers.host1.memory.usage', 2000.0, 1633035600, '2021-09-30', 1), 
('servers.host2.cpu.usage', 65.0, 1633035600, '2021-09-30', 1);

Вставляем записи с updated=2:
INSERT INTO graphite.data (metric, value, timestamp, date, updated) VALUES
('servers.host1.cpu.usage', 75.0, 1633035600, '2021-09-30', 2),  ('servers.host1.memory.usage', 1800.0, 1633035600, '2021-09-30', 2),('servers.host2.cpu.usage', 65.0, 1633035600, '2021-09-30', 2); 

Далее делаем:
> SELECT * FROM graphite.data;
> OPTIMIZE TABLE graphite.data;
> SELECT * FROM graphite.data;
