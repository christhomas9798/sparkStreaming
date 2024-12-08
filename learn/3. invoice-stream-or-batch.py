# Databricks notebook source
from pyspark.sql.functions import *
class invoiceStream():
    def __init__(self):
        self.base_dir='/FileStore/spark_streaming/json'
    def get_schema(self):

        return """InvoiceNumber string,CreatedTime bigint,StoreID string,PosID string,CashierID string,CustomerType string,CustomerCardNo string,TotalAmount double,NumberOfItems int,PaymentMethod string,TaxableAmount double,CGST double,SGST double,CESS double,DeliveryType string,DeliveryAddress struct<AddressLine string,City string,State string,PinCode string ,ContactNumber string>,InvoiceLineItems array<struct<ItemCode string,ItemDescription string,ItemPrice double,ItemQty bigint,TotalValue double>>"""
    
    def read_invoices(self):
        return spark.readStream.format('json').schema(self.get_schema()).load(f'{self.base_dir}/invoices')
    def explode_and_flatten_invoices(self, invoice_df):
        return invoice_df.withColumn('InvoiceLineItems',explode(col('InvoiceLineItems'))).withColumn('AddressLine',col('DeliveryAddress.AddressLine')).withColumn('City',col('DeliveryAddress.City')).withColumn('State',col('DeliveryAddress.State')).withColumn('PinCode',col('DeliveryAddress.PinCode')).withColumn('ContactNumber',col('DeliveryAddress.ContactNumber')).withColumn('ItemCode',col('InvoiceLineItems.ItemCode')).withColumn('ItemDescription',col('InvoiceLineItems.ItemDescription')).withColumn('ItemPrice',col('InvoiceLineItems.ItemPrice')).withColumn('ItemQty',col('InvoiceLineItems.ItemQty')).withColumn('TotalValue',col('InvoiceLineItems.TotalValue')).drop(col('InvoiceLineItems'),col('DeliveryAddress'))
    
    def appendInvoices(self,flattened_df,trigger:str):
        sQuery= flattened_df.writeStream.format('delta').option('checkpointLocation','/FileStore/spark_streaming/json/checkpoint/invoice').outputMode('append')
        if trigger=='stream':
            return sQuery.trigger(processingTime='20 seconds').toTable('invoice_line_items')
        else:
            return sQuery.trigger(availableNow=True).toTable('invoice_line_items')


    def execute_all(self,streamType):
        print(f'Start the job')
        print(f'Set the base dir = {self.base_dir}')
        print(f'Load the data')
        invoices_df=self.read_invoices()
        print(f'Explode and flatten the data')
        flattened_df=self.explode_and_flatten_invoices(invoices_df)
        print(f'Save the data to delta')
        sQuery=self.appendInvoices(flattened_df,streamType)
        print(f'Job completed')
        return sQuery

# COMMAND ----------

run=invoiceStream()

# COMMAND ----------

k=run.execute_all('stream')

# COMMAND ----------

k.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from invoice_line_items

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from invoice_line_items
