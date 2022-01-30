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

#### Configurations:
Update the configuration value for the following attributes in Constant class(under com.org.adjust.const)
```
//Postgres Connection Details
  val CONN_URL = "jdbc:postgresql://<server>:<port>/<database>"
  val USER_ID = "<username>"
  val PASSWORD = "<password>"
  val TABLE_NAME = "stage.weather_balloonraw_data"
  val PSG_DRIVER_NAME = "org.postgresql.Driver"

  //Destination Path
  val DESTINATION_PATH = "<destination_path>"

  //Source of the file path
  val FILE_SOURCE_LOCATION ="<source_path>"

  //Files Details
  val FILE_NAME_CONVENTION ="USM"
  val FILE_EXTENSION ="txt"
```
