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
  val TABLE_NAME = "stage.weather_balloon_rawdata_satge"
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
2. While loading data into the postgres auto generated row sequence id will be genearted for each record.![image](https://user-images.githubusercontent.com/36407457/151717121-5bec70b7-24d2-41a8-93ea-3856aca8d32b.png)
3. Spark consumes the data from the postgres table, perform the tranformations to map the headers with relavent meansurement data and also derives the attributes from rawdata.
+-----------+---------+--------------------------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+----------------+
|ID         |createdby|date_time                 |YEAR|MONTH|DAY|HOUR|RELTIME|NUMLEV|P_SRC|NP_SRC|LAT   |LON     |LVLTYP1|LVLTYP2|ETIME|PRESS|PFLAG|GPH |ZFLAG|TEMP |TFLAG|RH   |DPDP |WDIR|WSPD|GPH_BUCKET_WIDTH|
+-----------+---------+--------------------------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+----------------+
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |02 |99  |2310   |21    |     |      |550389|-1315778|3      |0      |2000 |-9999|     |3725|     |-9999|     |-9999|-9999|90  |103 |3000_4000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |06 |99  |1647   |17    |     |      |550389|-1315778|3      |0      |100  |-9999|     |251 |     |-9999|     |-9999|-9999|50  |45  |0_1000          |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |06 |99  |1647   |17    |     |      |550389|-1315778|3      |0      |1200 |-9999|     |2285|     |-9999|     |-9999|-9999|90  |80  |2000_3000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |11 |99  |2220   |19    |     |      |550389|-1315778|3      |0      |1000 |-9999|     |1925|     |-9999|     |-9999|-9999|310 |67  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |11 |99  |2220   |19    |     |      |550389|-1315778|3      |0      |1800 |-9999|     |3365|     |-9999|     |-9999|-9999|350 |89  |3000_4000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |23 |99  |1755   |26    |     |      |550389|-1315778|3      |0      |1300 |-9999|     |2465|     |-9999|     |-9999|-9999|60  |112 |2000_3000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |24 |99  |1854   |26    |     |      |550389|-1315778|3      |0      |1800 |-9999|     |3365|     |-9999|     |-9999|-9999|340 |139 |3000_4000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |24 |99  |2357   |11    |     |      |550389|-1315778|3      |0      |300  |-9999|     |647 |     |-9999|     |-9999|-9999|290 |36  |0_1000          |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|02   |28 |99  |2341   |15    |     |      |550389|-1315778|3      |0      |800  |-9999|     |1565|     |-9999|     |-9999|-9999|240 |85  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |02 |99  |2343   |11    |     |      |550389|-1315778|3      |0      |500  |-9999|     |1025|     |-9999|     |-9999|-9999|190 |139 |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |04 |99  |2252   |19    |     |      |550389|-1315778|3      |0      |200  |-9999|     |449 |     |-9999|     |-9999|-9999|210 |45  |0_1000          |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |09 |99  |1746   |11    |     |      |550389|-1315778|3      |0      |900  |-9999|     |1745|     |-9999|     |-9999|-9999|260 |67  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |10 |99  |2337   |15    |     |      |550389|-1315778|3      |0      |700  |-9999|     |1385|     |-9999|     |-9999|-9999|260 |130 |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |18 |99  |1826   |20    |     |      |550389|-1315778|3      |0      |1900 |-9999|     |3545|     |-9999|     |-9999|-9999|320 |170 |3000_4000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |23 |99  |1742   |18    |     |      |550389|-1315778|3      |0      |300  |-9999|     |647 |     |-9999|     |-9999|-9999|340 |76  |0_1000          |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |24 |99  |1825   |26    |     |      |550389|-1315778|3      |0      |900  |-9999|     |1745|     |-9999|     |-9999|-9999|220 |49  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |25 |99  |1819   |10    |     |      |550389|-1315778|3      |0      |700  |-9999|     |1385|     |-9999|     |-9999|-9999|270 |27  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|03   |27 |99  |2305   |19    |     |      |550389|-1315778|3      |0      |800  |-9999|     |1565|     |-9999|     |-9999|-9999|160 |156 |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|04   |02 |99  |1725   |25    |     |      |550389|-1315778|3      |0      |500  |-9999|     |1025|     |-9999|     |-9999|-9999|240 |36  |1000_2000       |
|USM00070398|adjust   |2022-01-30 15:18:06.870384|1942|04   |04 |99  |1758   |5     |     |      |550389|-1315778|3      |1      |0    |-9999|     |35  |     |-9999|     |-9999|-9999|340 |31  |0_1000          |
+-----------+---------+--------------------------+----+-----+---+----+-------+------+-----+------+------+--------+-------+-------+-----+-----+-----+----+-----+-----+-----+-----+-----+----+----+----------------+

4. GPH_BUCKET_WIDTH is derived for partitioning the data and at the end writes into destination location [/umesh/parquet/GPH_BUCKET_WIDTH=0_1000].
![image](https://user-images.githubusercontent.com/36407457/151718401-a5b280aa-f8b2-4b9c-9b0a-979c4e2443e8.png)



