# Databricks notebook source
def get_schema():
    return """InvoiceNumber string,CreatedTime bigint,StoreID string,PosID string,CashierID string,CustomerType string,CustomerCardNo string,TotalAmount double,NumberOfItems int,PaymentMethod string,TaxableAmount double,CGST double,SGST double,CESS double,DeliveryType string,DeliveryAddress struct<AddressLine string,City string,State string,PinCode string ,ContactNumber string>,InvoiceLineItems array<struct<ItemCode string,ItemDescription string,ItemPrice double,ItemQty bigint,TotalValue double>>"""

# COMMAND ----------

df=spark.read.format("json").schema(get_schema()).load('/FileStore/spark_streaming/kafka_invoice/invoices.json')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col,struct,to_json


# COMMAND ----------

ldf=df.select(col('StoreID').alias('key'),to_json(struct('*')).alias('value'))

# COMMAND ----------

display(ldf)

# COMMAND ----------

class Bronze:
    def __init__(self):
        self.base_data_dir = "/FileStore/spark_streaming/kafka_invoice"
        self.bootstrap_server = 'pkc-41mxj.uksouth.azure.confluent.cloud:9092'
        self.jaas_module = 'org.apache.kafka.common.security.plain.PlainLoginModule'
        self.username = 'QS2PTUBVDOCNVBYP'
        self.password = 'VtJfamx/PVd69a8q2UcXRZsZRIHfAaO9Rwceh3l9ln2cjlm1MDdxi51e6VwMIgkY'
    def get_schema(self):
        return """InvoiceNumber string,CreatedTime bigint,StoreID string,PosID string,CashierID string,CustomerType string,CustomerCardNo string,TotalAmount double,NumberOfItems int,PaymentMethod string,TaxableAmount double,CGST double,SGST double,CESS double,DeliveryType string,DeliveryAddress struct<AddressLine string,City string,State string,PinCode string ,ContactNumber string>,InvoiceLineItems array<struct<ItemCode string,ItemDescription string,ItemPrice double,ItemQty bigint,TotalValue double>>"""
    def read_invoices(self, condition):
        return spark.readStream.format("json").schema(self.get_schema()).load(f'{self.base_data_dir}').where(condition)
    def getKafkaMessage(self,df,key):
        return df.select(col(key).cast('string').alias('key'),to_json(struct('*')).alias('value'))
    def sendToKafka(self,kafka_df):
        return kafka_df.writeStream.format("kafka").option("kafka.bootstrap.servers", self.bootstrap_server).option("kafka.sasl.mechanism", "PLAIN").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.username}' password='{self.password}';").option("topic","invoices").option("checkpointLocation", f"{self.base_data_dir}/checkpoint/kafka_producer").outputMode("append").start()
    def process(self, condition):
        print('Starting Kafka Producer Stream')
        invoices_df=self.read_invoices(condition)
        kafka_df=self.getKafkaMessage(invoices_df,'StoreID')
        sQuery=self.sendToKafka(kafka_df)
        print('Done')
        return sQuery

# COMMAND ----------

run=Bronze()
sQ=run.process(col('StoreID')=='STR6162')
