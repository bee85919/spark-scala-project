###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

###
spark = SparkSession.builder.appName("CSV Merge").getOrCreate()

###
root_path = "/Users/b06/Desktop/yeardream/medi-05/spark-scala-project/output"
csvs_path = f"{root_path}/pyspark/naverplace"
save_path = f"{root_path}/result"

###
df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(csvs_path + "/*.csv")
df = df.dropDuplicates()
df.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv(save_path)

###
spark.stop()