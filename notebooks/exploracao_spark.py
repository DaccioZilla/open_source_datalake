# %%
from pyspark.sql import SparkSession

# %%
spark = SparkSession\
    .builder\
    .master('spark://spark-master:7077')\
    .appName('unity_catalog_test')\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark

# %%
spark.sql("SHOW CATALOGS").show()

# %%
spark.sql("SHOW SCHEMAS").show()


# %%
spark.sql("SHOW TABLES IN datalake.silver").show()

# %%
spark.read.parquet('s3a://datalake/silver/tweets').write.format('delta').save('s3a://unitycatalog/silver/tweets')

# %%
spark.sql('DROP TABLE bronze.table_on_s3')

# %%
spark.sql("""
  CREATE external TABLE 
  bronze.table_on_s3 (id INT, desc STRING)
  USING delta
  LOCATION 's3a://unitycatalog/bronze/table_on_s3'
""")

# %%
spark.sql('desc extended bronze.table_on_s3;')


# %%
spark.sql("insert into bronze.table_on_s3 values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');")


# %%
spark.sql('SELECT * FROM silver.tweets').show()

# %%
spark.read.parquet('s3a://datalake/silver/tweets').write.saveAsTable('datalake.silver.tweets', mode = 'append')

# %%
df = spark.read.json('s3a://datalake/bronze/twitter_datascience').write.saveAsTable('bronze.twitter_datascience')


# %%
df = spark.read.json('s3a://datalake/bronze/twitter_datascience')

# %%
df.printSchema()

# %%
from pyspark.sql import functions as f

# %%
df.select(f.explode("data")).printSchema()

# %%
df.select(f.explode("data")).show()

# %%
df.select(f.explode("data").alias("tweets"))\
.select("tweets.author_id", "tweets.conversation_id",
        "tweets.created_at", "tweets.id",
        "tweets.public_metrics.*", "tweets.text").printSchema()

# %%
tweet_df = df.select(f.explode("data").alias("tweets"))\
  .select("tweets.author_id", "tweets.conversation_id",
        "tweets.created_at", "tweets.id",
        "tweets.public_metrics.*", "tweets.text")

tweet_df.show()


# %%
process_date = '2023-11-11'
tweet_df = tweet_df.withColumn('process_date', f.to_date(f.lit(process_date)))
tweet_df.show()

# %%
tweet_df.show(5)

# %%
df.select(f.explode('includes.users'))

# %%
users = df.select(f.explode("includes.users").alias("users")).select("users.*").printSchema()

# %%
users.show(5)

# %%



