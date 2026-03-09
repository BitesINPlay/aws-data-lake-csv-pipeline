import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="csv_pipeline_db",
    table_name="processed_clean"
)

df = dyf.toDF()

df2 = (
    df.withColumn("amount", col("amount").cast(DoubleType()))
      .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
)

df2.write.mode("overwrite").parquet("s3://csv-final-data--bucket/final/")

job.commit()
