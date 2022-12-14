#Write spark code to Read data from mysql , do some processing and store output data back to hive


from pyspark import SparkConf
#from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark = SparkSession.\
         builder\
        .config("spark.jars", "C:\\Program Files (x86)\\MySQL\\Connector_J_8.0\\mysql-connector-j-8.0.31.jar") \
        .master("local") \
        .appName("Project_1") \
        .enableHiveSupport() \
        .getOrCreate()

filmDF = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/sakila") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "film") \
    .option("user", "root")\
    .option("password", "root").load()

filmDF.printSchema()
filmDF.createOrReplaceTempView("filmTable")

filmDF.show()
rental_2 = filmDF.filter(filmDF["rental_duration"] > 2)

rental_2.show()

spark.sql("select avg(rental_duration), description from filmTable group by description").show()

spark.sql("select avg(rental_rate), avg(length) from filmTable where replacement_cost >=10 AND replacement_cost<= 20").show()


spark.sql("select title, film_id from filmTable order by title Desc").show()

filmDF.write.format("csv").mode('overwrite').save("filmCSV")