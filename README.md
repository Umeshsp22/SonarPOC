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

## Data Flow

1. Engine Reads files from the specified source location and loads one by one into stage with full refresh, eventually taking old files data into history. 
2. While loading data into the postgres auto generated row sequence id will be genearted by deafult for each record.![image](https://user-images.githubusercontent.com/36407457/151717121-5bec70b7-24d2-41a8-93ea-3856aca8d32b.png)
3. Spark consumes the data from the postgres table, perform the tranformations to map the headers with relavent meansurement data and also derives the attributes from rawdata.
+-----------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+------------+
|ID         |YEAR|MONTH|DAY|HOUR|RELTIME|NUMLEV|P_SRC|NP_SRC|LAT   |LON     |LVLTYP1|LVLTYP2|ETIME|PRESS|PFLAG|GPH |ZFLAG|TEMP |TFLAG|RH   |DPDP |WDIR|WSPD|bucket      |
+-----------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+------------+
|USM00070398|1942|02   |02 |99  |2310   |21    |     |      |550389|-1315778|3      |0      |2000 |-9999|     |3725|     |-9999|     |-9999|-9999|90  |103 |3000-to-4000|
|USM00070398|1942|02   |03 |99  |1640   |14    |     |      |550389|-1315778|3      |0      |100  |-9999|     |251 |     |-9999|     |-9999|-9999|290 |13  |0-to-1000   |
|USM00070398|1942|02   |03 |99  |2210   |37    |     |      |550389|-1315778|3      |0      |500  |-9999|     |1025|     |-9999|     |-9999|-9999|40  |67  |1000-to-2000|
|USM00070398|1942|02   |03 |99  |2210   |37    |     |      |550389|-1315778|3      |0      |1700 |-9999|     |3185|     |-9999|     |-9999|-9999|80  |112 |3000-to-4000|
|USM00070398|1942|02   |03 |99  |2210   |37    |     |      |550389|-1315778|3      |0      |3200 |-9999|     |6065|     |-9999|     |-9999|-9999|60  |98  |6000-to-7000|
|USM00070398|1942|02   |04 |99  |1800   |26    |     |      |550389|-1315778|3      |0      |2200 |-9999|     |4085|     |-9999|     |-9999|-9999|340 |143 |4000-to-5000|
|USM00070398|1942|02   |05 |99  |2205   |8     |     |      |550389|-1315778|3      |0      |600  |-9999|     |1205|     |-9999|     |-9999|-9999|40  |148 |1000-to-2000|
|USM00070398|1942|02   |06 |99  |1647   |17    |     |      |550389|-1315778|3      |0      |100  |-9999|     |251 |     |-9999|     |-9999|-9999|50  |45  |0-to-1000   |
|USM00070398|1942|02   |06 |99  |1647   |17    |     |      |550389|-1315778|3      |0      |1200 |-9999|     |2285|     |-9999|     |-9999|-9999|90  |80  |2000-to-3000|
|USM00070398|1942|02   |06 |99  |2238   |26    |     |      |550389|-1315778|3      |0      |100  |-9999|     |251 |     |-9999|     |-9999|-9999|320 |22  |0-to-1000   |
|USM00070398|1942|02   |07 |99  |2225   |7     |     |      |550389|-1315778|3      |0      |600  |-9999|     |1205|     |-9999|     |-9999|-9999|350 |219 |1000-to-2000|
|USM00070398|1942|02   |08 |99  |2228   |14    |     |      |550389|-1315778|3      |0      |900  |-9999|     |1745|     |-9999|     |-9999|-9999|150 |112 |1000-to-2000|
|USM00070398|1942|02   |11 |99  |2220   |19    |     |      |550389|-1315778|3      |0      |1000 |-9999|     |1925|     |-9999|     |-9999|-9999|310 |67  |1000-to-2000|
|USM00070398|1942|02   |11 |99  |2220   |19    |     |      |550389|-1315778|3      |0      |1800 |-9999|     |3365|     |-9999|     |-9999|-9999|350 |89  |3000-to-4000|
|USM00070398|1942|02   |12 |99  |1825   |19    |     |      |550389|-1315778|3      |0      |300  |-9999|     |647 |     |-9999|     |-9999|-9999|180 |13  |0-to-1000   |
|USM00070398|1942|02   |21 |99  |1605   |21    |     |      |550389|-1315778|3      |0      |800  |-9999|     |1565|     |-9999|     |-9999|-9999|350 |85  |1000-to-2000|
|USM00070398|1942|02   |21 |99  |2240   |22    |     |      |550389|-1315778|3      |0      |1200 |-9999|     |2285|     |-9999|     |-9999|-9999|340 |85  |2000-to-3000|
|USM00070398|1942|02   |21 |99  |2240   |22    |     |      |550389|-1315778|3      |0      |1300 |-9999|     |2465|     |-9999|     |-9999|-9999|340 |94  |2000-to-3000|
|USM00070398|1942|02   |22 |99  |2230   |26    |     |      |550389|-1315778|3      |0      |1000 |-9999|     |1925|     |-9999|     |-9999|-9999|60  |72  |1000-to-2000|
|USM00070398|1942|02   |22 |99  |2230   |26    |     |      |550389|-1315778|3      |0      |1600 |-9999|     |3005|     |-9999|     |-9999|-9999|20  |89  |3000-to-4000|
+-----------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+------------+
4. GPH_BUCKET_WIDTH is derived for partition the data and at the end writes into destination location.


