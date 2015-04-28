
CREATE EXTERNAL TABLE minute_bars
( 
	id STRUCT<oid:STRING, bsontype:INT>,
	Symbol STRING,
	Timestamp STRING,
	Day INT,
	Open DOUBLE,
	High DOUBLE,
	Low DOUBLE,
	Close DOUBLE,
	Volume INT
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id",
 "Symbol":"Symbol", "Timestamp":"Timestamp", "Day":"Day", "Open":"Open", "High":"High", "Low":"Low", "Close":"Close", "Volume":"Volume"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/marketdata.minbars');

CREATE TABLE five_minute_bars
( 
	id STRUCT<oid:STRING, bsontype:INT>,
	Symbol STRING,
	Timestamp STRING,
	Open DOUBLE,
	High DOUBLE,
	Low DOUBLE,
	Close DOUBLE
);

INSERT INTO TABLE five_minute_bars 
SELECT m.id, m.Symbol, m.OpenTime as Timestamp, m.Open, m.High, m.Low, m.Close 
FROM
	(SELECT 
		id,
		Symbol,
		FIRST_VALUE(Timestamp)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as OpenTime, 
		LAST_VALUE(Timestamp)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as CloseTime, 
		FIRST_VALUE(Open)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as Open, 	
		MAX(High)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as High, 
		MIN(Low)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as Low,
		LAST_VALUE(Close)
		OVER (
			PARTITION BY floor(unix_timestamp(Timestamp, 'yyyy-MM-dd HH:mm')/(5*60))
			 			ORDER BY Timestamp) 
		as Close
	FROM minute_bars) as m
WHERE unix_timestamp(m.CloseTime, 'yyyy-MM-dd HH:mm') - unix_timestamp(m.OpenTime, 'yyyy-MM-dd HH:mm') = 60*4;
