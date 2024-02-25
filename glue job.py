import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import split, col, when, udf
from pyspark.sql.types import FloatType, StringType, IntegerType, DoubleType
from awsglue.job import Job

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Create a dynamic frame from the Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="amazon-sales-sk",
    table_name="cleanedfiles",
    transformation_ctx="read_from_glue_catalog"
)

# Convert the dynamic frame to a DataFrame
df = dynamic_frame.toDF()

# Load the data into a DataFrame
df = spark.read.parquet("s3://amazonsales-capstone-sk/cleanedfiles/")

# Handle null values by replacing them with zeros
df = df.na.fill(0)

# Extract brand name from the first word of the product_name
df = df.withColumn("product_brand_name", split(col("product_name"), " ")[0])

# Convert discount_percentage to double and remove % symbol
df = df.withColumn("discount_percentage(%)", col("discount_percentage").substr(1, 2).cast(DoubleType()))

# Convert rating to double and handle cases where it is empty or not a valid number
def parse_rating(rating):
    try:
        return float(rating)
    except ValueError:
        return None

parse_rating_udf = udf(parse_rating, DoubleType())

df = df.withColumn("rating_value", parse_rating_udf(col("rating")))

# Calculate above_4_rating, 3to4_rating, and bad_review_percentage
df = df.withColumn("above_4_rating", when(col("rating_value") > 4, 1).otherwise(0)) \
    .withColumn("3to4_rating", when((col("rating_value") >= 3) & (col("rating_value") <= 4), 1).otherwise(0)) \
    .withColumn("bad_review", when((col("above_4_rating") + col("3to4_rating")) == 0, 1).otherwise(0)) \

# Calculate top performers
from pyspark.sql.functions import desc
top_performers_list = df.groupBy("product_id").count().orderBy(desc("count")).limit(10).select("product_id").collect()
top_performers_list = [row.product_id for row in top_performers_list]
df = df.withColumn("top_performer", col("product_id").isin(top_performers_list).cast(IntegerType()))

# Split the 'category' column into separate columns
df = df.withColumn("category_levels", split("category", "\|"))
df = df.select(
    "*",
    col("category_levels")[0].alias("main_category"),
    col("category_levels")[1].alias("sub_category1"),
    col("category_levels")[2].alias("sub_category2"),
    col("category_levels")[3].alias("sub_category3"),
    col("category_levels")[4].alias("sub_category4")
).drop("category", "category_levels")

# Drop unnecessary columns
drop_columns = ['rating_value','discount_percentage','user_id','product_name','user_name', 'review_id', 'review_title', 'review_content', 'img_link', 'product_link', 'about_product']
df_final = df.drop(*drop_columns)

# Write the transformed data back to S3
df_final.coalesce(1).write.mode("overwrite").parquet("s3://amazonsales-capstone-sk/transformed/")

job.commit()
