# %%
from pyspark.sql import SparkSession

spark = SparkSession\
            .builder\
            .master('spark://spark-master:7077')\
            .appName('exploracao_silver')\
            .getOrCreate()

# %%
spark

# %%
tweet_df = spark.read.format('delta').load('s3a://datalake/silver/tweets/process_date=2023-11-06')
tweet_df.printSchema()

# %%
import pyspark.sql.functions as f

tweet_conversas = tweet_df.alias('tweet')\
                    .groupBy(f.to_date('created_at').alias('created_date'))\
                    .agg(
                        f.countDistinct('author_id').alias('n_tweets'),
                        f.sum('like_count').alias('n_like'),
                        f.sum('quote_count').alias('n_quote'),
                        f.sum('reply_count').alias('n_like'),
                        f.sum('retweet_count').alias('n_retweet')
                    )\
                    .withColumn("weekday", f.date_format("created_date", "E"))
tweet_conversas.show(10)

# %%



