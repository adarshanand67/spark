# Apache Spark

Apache Spark is an analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general execution graphs.

It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming for stream processing.

## Key Features

- **Speed**: Spark processes data in memory, making it much faster than traditional disk-based processing.
- **Ease of Use**: High-level APIs in Java, Scala, Python, and R, and an interactive shell.
- **General Execution Model**: Supports a wide range of applications, from batch processing to streaming, machine learning, and graph processing.
- **Advanced Analytics**: Includes libraries for SQL, machine learning, graph computation, and stream processing.

For more details, you can refer to the [Taming Big Data with Apache Spark](https://intel.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/3708614#content) course on Udemy.

# Order of Course contents

- Intro to spark architechture
- RDD (Resilient Distributed Dataset)
- Transformation on RDD - map, flatmap, filter, distinct, sample, union, intersection, subtract, cartesian, groupByKey, reduceByKey, aggregateByKey, sortByKey
- Actions on RDD - collect, count, take, reduce, saveAsTextFile, countByKey, foreach
- SparkSQL - Dataframe, Dataset, SQL queries
- Transformations on Dataframe - select, filter, groupBy, sort, join, union, dropDuplicates, withColumn, withColumnRenamed, drop, describe, printSchema
- joins on dataframes
- Broadcast variables
- degrees of separation
- item based collaborative filtering, cache(), persist()
- recommender systems (cosine similarity, pearson correlation)
- Running Spark on cluster (Amazon Elastic Mapreduce cloud) [turn off clusters when not in use]
- troubleshoot (spark history server)
- dataframes spark.ml (machine learning library)
- spark ml als (alternating least squares) for recommendation system, linear regression, logistic regression, decision tree, random forest, gradient boosting, k-means clustering
- spark streaming - processing log data real time, hdfs, kafka, checkpointing (fault tolerance), dstream, microbatching, sliding window, stateful transformations

- graphx (graph processing)