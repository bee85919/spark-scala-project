from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("MergeCSVs") \
    .getOrCreate()

root_path = "/Users/b06/Desktop/yeardream/medi-05/spark-scala-project/output/pyspark"
input_path = f"{root_path}/hospital_df"
output_path = f"{root_path}/merged_hospital_df"


df = spark.read.csv(input_path, header=True, inferSchema=True)
df.coalesce(1).write.csv(output_path, mode="overwrite", header=True)