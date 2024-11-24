# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC  %run ./02-setup #scripts create database and necessary tables, and clean up

# COMMAND ----------

SH = SetupHelper(env) #set up functions for creating database and tables
SH.cleanup() #erase existing directories and database

# COMMAND ----------

dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"}) 

# COMMAND ----------

# MAGIC %run ./03-history-loader 

# COMMAND ----------

HL = HistoryLoader(env) #insert data to reference tables, specifically date look up in this case
SH.validate() #check all tables were created in database   
HL.validate() #validate the data load, specifically for date look up in this case, that there are 365 records

# COMMAND ----------

# MAGIC %run ./10-producer 

# COMMAND ----------

PR =Producer() #Class defines functions to bring test data from test data directory to landing zone
PR.produce(1) #Bring data set 1 for each data source
PR.validate(1) #validate number of records in the bronze tables for the test set
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"}) #run script sets up classes from Bronze, Silver, Gold and consumes data to those tables

# COMMAND ----------

# MAGIC %run ./04-bronze #run script so we can run the validation function

# COMMAND ----------

# MAGIC %run ./05-silver #run script so we can run the validation function

# COMMAND ----------

# MAGIC %run ./06-gold #run script so we can run the validation function

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)
BZ.validate(1)  #validate number of records in bronze tables after test set 1 was consumed
SL.validate(1)  #validate number of records in silver tables after test set 1 was consumed
GL.validate(1)  #validate number of records in gold tables after test set 1 was consumed

# COMMAND ----------

PR.produce(2)
PR.validate(2)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

BZ.validate(2)
SL.validate(2)
GL.validate(2)
SH.cleanup()
