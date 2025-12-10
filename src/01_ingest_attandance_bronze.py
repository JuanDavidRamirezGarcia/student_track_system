# Databricks notebook source

import pandas as pd


full_table_path = "student_system.attendance.raw_attendance"

csv_path = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSx7rjuzgzIh2LPHGsYJIMSsx9q_bL_MYwvc49yi1Bvo5I9ISwQkRIt59aro2ChXza4-JQ8Br-5akgM/pub?gid=0&single=true&output=csv"

df = pd.read_csv(csv_path)

raw_attendance_df = spark.createDataFrame(df)

raw_attendance_df.write.mode("overwrite").saveAsTable(full_table_path)

display(raw_attendance_df.limit(10))

# COMMAND ----------

# Vizualice data type
raw_attendance_df.printSchema()
