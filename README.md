## Prerequisite
#### Execute DDL in Postgres:
```
--DDL to create stage table
create table stage.weather_balloon_rawdata_stage(
id SERIAL UNIQUE primary key, 
raw_data varchar,
createdby varchar default 'adjust',
date_time  timestamp default current_timestamp
);

--DDL to create history table
create table stage.weather_balloon_rawdata_history(
id int, 
raw_data varchar,
createdby varchar,
date_time  timestamp
);
```
