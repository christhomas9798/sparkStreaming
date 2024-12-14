# Databricks notebook source
print(1)

# COMMAND ----------

bootstrap_server ='pkc-41mxj.uksouth.azure.confluent.cloud:9092'
jaas_module='org.apache.kafka.common.security.plain.PlainLoginModule'
username='QS2PTUBVDOCNVBYP'
password= 'VtJfamx/PVd69a8q2UcXRZsZRIHfAaO9Rwceh3l9ln2cjlm1MDdxi51e6VwMIgkY'


# COMMAND ----------

df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f"{jaas_module} required username='{username}' password='{password}';") \
    .option("subscribe", "invoices") \
    .load()

# COMMAND ----------



# COMMAND ----------

display(df)
