import sys.process._
"wget -P /tmp https://www.gakhov.com/static/www/datasets/stratahadoop-BCN-2014.json" !!

val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))

val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")
val rdd = df.select("text").rdd.map(row => row.getString(0))
rdd.take(10).foreach(println)
val wordCounts = rdd.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a+b)

wordCounts.take(10).foreach(println)

val hashTag = rdd.flatMap(word => word.split(" ")).filter(word => word.startsWith("#"))
val nbHashTag = hashTag.count()
print("There are: "+nbHashTag+" hashtags\n")
val hashcount=hashTag.map(word => (word,1)).reduceByKey((a,b) => a+b)
hashcount.take(10).foreach(println)
print("\n")
val frequentHashTag = hashTag.map(word =>(word,1)).reduceByKey((a,b) => a+b).sortBy(_._2,false)
frequentHashTag.take(10).foreach(println)

val user_tweets = df.select($"user.id",$"user.name").rdd.map(word =>(word,1)).reduceByKey((a,b) => a+b).sortBy(_._2,false)
user_tweets.take(10).foreach(println)

