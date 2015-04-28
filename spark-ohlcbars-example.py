
# set up parameters for reading from MongoDB via Hadoop input format
config = {"mongo.input.uri": "mongodb://localhost:27017/marketdata.minbars"}
inputFormatClassName = "com.mongodb.hadoop.MongoInputFormat"

# these values worked but others might as well
keyClassName = "org.apache.hadoop.io.Text"
valueClassName = "org.apache.hadoop.io.MapWritable"
 
# read the 1-minute bars from MongoDB into Spark RDD format
minBarRawRDD = sc.newAPIHadoopRDD(inputFormatClassName, keyClassName, valueClassName, None, None, config)

# configuration for output to MongoDB
config["mongo.output.uri"] = "mongodb://localhost:27017/marketdata.fiveminutebars"
outputFormatClassName = "com.mongodb.hadoop.MongoOutputFormat"
 
# takes the verbose raw structure (with extra metadata) and strips down to just the pricing data
minBarRDD = minBarRawRDD.values()
 
import calendar, time, math
 
dateFormatString = '%Y-%m-%d %H:%M'
 
# sort by time and then group into each bar in 5 minutes
groupedBars = minBarRDD.sortBy(lambda doc: str(doc["Timestamp"])).groupBy(lambda doc: 
	(doc["Symbol"], math.floor(calendar.timegm(time.strptime(doc["Timestamp"], dateFormatString)) / (5*60))))

# define function for looking at each group and pulling out OHLC
# assume each grouping is a tuple of (symbol, seconds since epoch) and a resultIterable of 1-minute OHLC records in the group

# write function to take a (tuple, group); iterate through group; and manually pull OHLC
def ohlc(grouping):
	low = sys.maxint
	high = -sys.maxint
	i = 0
	groupKey = grouping[0]
	group = grouping[1]
	for doc in group:
		#take time and open from first bar
		if i == 0:
			openTime = doc["Timestamp"]
			openPrice = doc["Open"]
		#assign min and max from the bar if appropriate
		if doc["Low"] < low:
			low = doc["Low"]
		if doc["High"] > high:
			high = doc["High"]
		i = i + 1			
		# take close of last bar
		if i == len(group):
			close = doc["Close"]
	outputDoc = {"Symbol": groupKey[0], 
		"Timestamp": openTime, 
		"Open": openPrice,
		"High": high,
		"Low": low,
		"Close": close}
	# tried returning [None, outputDoc] and seemed exception earlier
	return (None, outputDoc)


resultRDD = groupedBars.map(ohlc)

# This causes ClassCastException apparently because of an issue in Spark logged as SPARK-5361.  Should write to MongoDB but could not test.
# resultRDD.saveAsNewAPIHadoopFile("file:///placeholder", outputFormatClassName, None, None, None, None, config)
