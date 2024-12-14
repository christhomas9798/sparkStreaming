# Databricks notebook source
dbutils.fs.rm("/FileStore/spark_streaming/kafka_invoice/bronze/checkpoint",True)

# COMMAND ----------

from pyspark.sql.functions import col,from_json

# COMMAND ----------

class Bronze:
    def __init__(self):
        self.base_data_dir="/FileStore/spark_streaming/kafka_invoice"
        self.bootstrap_server ='pkc-41mxj.uksouth.azure.confluent.cloud:9092'
        self.jaas_module='org.apache.kafka.common.security.plain.PlainLoginModule'
        self.username='QS2PTUBVDOCNVBYP'
        self.password= 'VtJfamx/PVd69a8q2UcXRZsZRIHfAaO9Rwceh3l9ln2cjlm1MDdxi51e6VwMIgkY'
    def ingestFromKafka(self, startingOffsets="earliest"):
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.username}' password='{self.password}';") \
            .option("subscribe", "invoices") \
            .option("startingOffsets", startingOffsets) \
            .option("maxOffsetsPerTrigger", 100) \
            .load()
    def get_schema(self):

        return """InvoiceNumber string,CreatedTime bigint,StoreID string,PosID string,CashierID string,CustomerType string,CustomerCardNo string,TotalAmount double,NumberOfItems int,PaymentMethod string,TaxableAmount double,CGST double,SGST double,CESS double,DeliveryType string,DeliveryAddress struct<AddressLine string,City string,State string,PinCode string ,ContactNumber string>,InvoiceLineItems array<struct<ItemCode string,ItemDescription string,ItemPrice double,ItemQty bigint,TotalValue double>>"""
    def getInvoices(self,kafka_df):
        return kafka_df.select(col('key').cast('string').alias('key'),from_json(col('value').cast('string'),self.get_schema()).alias('value'),col('topic'),col('timestamp'))
    def process(self,timestamp=1):
        print(f'Starting Bronze Stream')
        kafka_df=self.ingestFromKafka('earliest')
        invoices_df=self.getInvoices(kafka_df)
        sQuery=invoices_df.writeStream.format("delta").option("checkpointLocation", f"{self.base_data_dir}/bronze/checkpoint").trigger(processingTime='10 seconds').outputMode("append").toTable('kafka_invoices_bronze')
        print(f'Done')
        return sQuery
    
    


# COMMAND ----------

run=Bronze()
run.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kafka_invoices_bronze order by `timestamp` desc
