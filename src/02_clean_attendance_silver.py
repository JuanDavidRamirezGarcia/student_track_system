# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_name = "student_system"
schema_name = "attendance"

source_table = f"{catalog_name}.{schema_name}.raw_attendance"
target_table = f"{catalog_name}.{schema_name}.silver_attendance"

df_raw = spark.read.table(source_table)

attendance_window = Window.partitionBy("student_name").orderBy("date")

df_silver = (
    df_raw
    .withColumn("date", F.to_date("date"))
    .withColumn("student_name", F.upper("student_name"))
    .withColumn("student_name", F.regexp_replace("student_name", "Á", "A"))
    .withColumn("student_name", F.regexp_replace("student_name", "É", "E"))
    .withColumn("student_name", F.regexp_replace("student_name", "Í", "I"))
    .withColumn("student_name", F.regexp_replace("student_name", "Ó", "O"))
    .withColumn("student_name", F.regexp_replace("student_name", "Ú", "U"))
    .withColumn(
        "status",
        F.when(F.lower(F.trim(F.col("status"))) == "yes", 1)
         .when(F.lower(F.trim(F.col("status"))) == "no", 0)
         .otherwise(F.lit(None)) 
    )
    .withColumn("grade", F.regexp_replace("grade", "a", "A"))
    .withColumn("grade", F.regexp_replace("grade", "b", "B"))
    .withColumn("grade", F.regexp_replace("grade", "c", "C"))
                
    .withColumn("prev_status", F.lag("status").over(attendance_window))
    
    .withColumn("final_status", 
        F.when(
            F.col("status").isNull(), # FIX: Only overwrite if NULL. Trust the 0!
            F.coalesce(F.col("prev_status"), F.lit(0))
        )
        .otherwise(F.col("status"))
    )
    
    .drop("status") 
    .withColumnRenamed("final_status", "status")        
)

df_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

df_silver.printSchema()

# COMMAND ----------

display(df_silver)