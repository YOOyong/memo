import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format


# https://dbmstutorials.com/pyspark/spark-dataframe-write-modes.html#Append
# https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
# https://www.youtube.com/watch?v=_C8kWso4ne4&t=5883s

### remove exsiting parquet file


### making parquet file
for root, d , files in os.walk('/haflow/ftp_tmp/' + job_run_id):
    for i, file in enumerate(files):
        path = os.path.join(root, file)
        hdfs_path = 'hdfs: 11111' + path[1:]
        df = spark.read.option('header','true').csv('./temp_data/train.csv')
        
        # pyspark 전처리
        df = df.na.drop(subset = ['Serial no'])
        df = df.drop(*['x','y']) #컬럼 없어도 error 안남.
        df = df.withColumnRenamed('Data time', 'Date time') #컬럼 없어도 error 안남.
        
        df = df.withColumn("dt",date_format(to_date("Date time"),'yyyyMMdd'))
        newColumns = list(map(lambda x : x.lower().replace(' ','_'),df.columns))
        df = df.toDF(*newColumns)
        if i == 0:
            df.printSchema()
        
        df.write.mode('append').partitionBy('dt').parquet('./temp_parquet/')