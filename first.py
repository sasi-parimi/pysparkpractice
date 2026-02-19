import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)




# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Sasi Parimi\.jdks\corretto-1.8.0_482'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
# URL -- DATA READS


from pyspark.sql.functions import explode

url = "https://randomuser.me/api/0.8/?results=10"


with urllib.request.urlopen(url) as response:
    jsonurldata = response.read().decode('utf-8')

json_rdd = sc.parallelize([jsonurldata])

df = spark.read.json(json_rdd)

# Explode the results array
df_exploded = df.select(
    "nationality",
    "seed",
    "version",
    explode("results").alias("results_exploded")
)

# Flatten the exploded results (individually calling columns out)
flat_df = df_exploded.select(
    "nationality",
    "seed",
    "version",
    "results_exploded.user.cell",
    "results_exploded.user.dob",
    "results_exploded.user.email",
    "results_exploded.user.gender",
    "results_exploded.user.location.city",
    "results_exploded.user.location.state",
    "results_exploded.user.location.street",
    "results_exploded.user.location.zip",
    "results_exploded.user.md5",
    "results_exploded.user.name.first",
    "results_exploded.user.name.last",
    "results_exploded.user.name.title",
    "results_exploded.user.password",
    "results_exploded.user.phone",
    "results_exploded.user.picture.large",
    "results_exploded.user.picture.medium",
    "results_exploded.user.picture.thumbnail",
    "results_exploded.user.registered",
    "results_exploded.user.salt",
    "results_exploded.user.sha1",
    "results_exploded.user.sha256",
    "results_exploded.user.username"
)

flat_df.show()
flat_df.printSchema()