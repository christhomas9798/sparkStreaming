# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

class bronze():
    def __init__(self):
        self.base_dir='/FileStore/spark_streaming/json'
    def get_schema(self):

        return """InvoiceNumber string,CreatedTime bigint,StoreID string,PosID string,CashierID string,CustomerType string,CustomerCardNo string,TotalAmount double,NumberOfItems int,PaymentMethod string,TaxableAmount double,CGST double,SGST double,CESS double,DeliveryType string,DeliveryAddress struct<AddressLine string,City string,State string,PinCode string ,ContactNumber string>,InvoiceLineItems array<struct<ItemCode string,ItemDescription string,ItemPrice double,ItemQty bigint,TotalValue double>>"""
    
    def read_invoices(self):
        return spark.readStream.format('json').schema(self.get_schema())\
            .option('cleanSource','delete')\
                .option('sourceArchiveDir',f'{self.base_dir}/archive/invoices')\
                    .load(f'{self.base_dir}/invoices')
                    # .option('cleanSource','delete')
    def process(self):
        print('Starting bronze stream')
        invoices_df=self.read_invoices()
        sQuery=invoices_df.writeStream.format('delta').option('checkpointLocation','/FileStore/spark_streaming/json/checkpoint/invoice_bronze').option('queryName','bronze-ingestion').outputMode('append').trigger(processingTime='20 seconds').toTable('invoices_bronze')
        print( 'Bronze stream completed')
        return sQuery

    

# COMMAND ----------

class silver():
    # def __init__(self):
    #     self.base_dir='/FileStore/spark_streaming/json'
    def read_invoices(self):
        return spark.readStream.table('invoices_bronze')
    def explode_and_flatten_invoices(self, invoice_df):
        return invoice_df.withColumn('InvoiceLineItems',explode(col('InvoiceLineItems'))).withColumn('AddressLine',col('DeliveryAddress.AddressLine')).withColumn('City',col('DeliveryAddress.City')).withColumn('State',col('DeliveryAddress.State')).withColumn('PinCode',col('DeliveryAddress.PinCode')).withColumn('ContactNumber',col('DeliveryAddress.ContactNumber')).withColumn('ItemCode',col('InvoiceLineItems.ItemCode')).withColumn('ItemDescription',col('InvoiceLineItems.ItemDescription')).withColumn('ItemPrice',col('InvoiceLineItems.ItemPrice')).withColumn('ItemQty',col('InvoiceLineItems.ItemQty')).withColumn('TotalValue',col('InvoiceLineItems.TotalValue')).drop(col('InvoiceLineItems'),col('DeliveryAddress'))
    
    def appendInvoices(self,flattened_df):
        return flattened_df.writeStream.format('delta').option('queryName','silver-ingestion').option('checkpointLocation','/FileStore/spark_streaming/json/checkpoint/invoice_silver').outputMode('append').trigger(processingTime='20 seconds').toTable('invoices_silver')
    
    def execute_all(self):
        print(f'Starting silver stream')
        # print(f'Set the base dir = {self.base_dir}')
        print(f'Load the data')
        invoices_df=self.read_invoices()
        print(f'Explode and flatten the data')
        flattened_df=self.explode_and_flatten_invoices(invoices_df)
        print(f'Save the data to delta')
        sQuery=self.appendInvoices(flattened_df)
        print(f'Silver stream completed')
        return sQuery

# COMMAND ----------

run_bronze=bronze()
run_bronze.process()

# COMMAND ----------

run_silver=silver()
run_silver.execute_all()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from invoices_silver
