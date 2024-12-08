# Databricks notebook source
print('hi')

# COMMAND ----------

dbutils.fs.mv('/FileStore/spark_streaming/test','/FileStore/spark_streaming/text_data_1.txt')

# COMMAND ----------

dbutils.fs.ls('/FileStore/spark_streaming')


# COMMAND ----------

class batchWC():
    def __init__(self):
        self.base_dir='/FileStore/spark_streaming'
    def load_data(self):
        lines_df=spark.read.format('text').option('lineSep','.').load(f'{self.base_dir}/')
        return lines_df
    def convert_lines_to_words(self,lines_df):
        from pyspark.sql.functions import explode,col,split,trim,lower,upper
        words_df=lines_df.select(explode(split(lines_df.value,' ')).alias('words'))
        return words_df
    def filter_and_clean_words(self,words_df):
        from pyspark.sql.functions import explode,col,split,trim,lower,upper
        wordsTrim=words_df.select(lower(trim(col('words'))).alias('words')).filter(col('words').isNotNull()).filter(col('words')!='').filter(col('words').rlike('^[a-zA-Z]+$'))
        final_df=wordsTrim.groupBy(col('words')).count().orderBy(col('count').desc())
        return final_df
    def save_to_delta(self,final_df):
        final_df.write.mode('overwrite').format('delta').saveAsTable('word_count_table')
    def execute_every_thing(self):
        print(f'Start the job')
        print(f'Set the base dir = {self.base_dir}')
        print(f'Load the data')
        lines_df=self.load_data()
        print(f'Convert the lines to words')
        words_df=self.convert_lines_to_words(lines_df)
        print(f'Filter and clean the words')
        final_df=self.filter_and_clean_words(words_df)
        print(f'Save the data to delta')
        self.save_to_delta(final_df)
        print(f'Job completed')






# COMMAND ----------

run=batchWC()
run.execute_every_thing()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(count) from word_count_table

# COMMAND ----------

class streamWC():
    def __init__(self):
        self.base_dir='/FileStore/spark_streaming'
    def load_data(self):
        lines_df=spark.readStream.format('text').option('lineSep','.').load(f'{self.base_dir}/')
        return lines_df
    def convert_lines_to_words(self,lines_df):
        from pyspark.sql.functions import explode,col,split,trim,lower,upper
        words_df=lines_df.select(explode(split(lines_df.value,' ')).alias('words'))
        return words_df
    def filter_and_clean_words(self,words_df):
        from pyspark.sql.functions import explode,col,split,trim,lower,upper
        wordsTrim=words_df.select(lower(trim(col('words'))).alias('words')).filter(col('words').isNotNull()).filter(col('words')!='').filter(col('words').rlike('^[a-zA-Z]+$'))
        final_df=wordsTrim.groupBy(col('words')).count().orderBy(col('count').desc())
        return final_df
    def save_to_delta(self,final_df):
        return final_df.writeStream.outputMode('complete').\
            option('checkpointLocation',f'{self.base_dir}/checkpoint/word_count').format('delta').toTable('word_count_table')
    def execute_every_thing(self):
        print(f'Start the job')
        print(f'Set the base dir = {self.base_dir}')
        print(f'Load the data')
        lines_df=self.load_data()
        print(f'Convert the lines to words')
        words_df=self.convert_lines_to_words(lines_df)
        print(f'Filter and clean the words')
        final_df=self.filter_and_clean_words(words_df)
        print(f'Save the data to delta')
        sQuery=self.save_to_delta(final_df)
        print(f'Job completed')
        return sQuery






# COMMAND ----------

j=streamWC()
l=j.execute_every_thing()
print(l)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(count) from word_count_table
