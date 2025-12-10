# Databricks notebook source
from pyspark.sql import functions as F

catalog_name = "student_system"
schema_name = "attendance"

source_silver = f"{catalog_name}.{schema_name}.silver_attendance"
target_gold   = f"{catalog_name}.{schema_name}.gold_student_performance"

# Read from the Cleaned Silver Table
df_clean = spark.read.table(source_silver)

# Aggregate (Group By) to create Business Metrics
df_gold = (
    df_clean
    .groupBy("student_name")
    .agg(
        # KPI 1: Total Days Recorded
        F.count("date").alias("total_class_days"),
        
        # KPI 2: Days Present (Summing the 1s we created in Silver)
        F.sum("status").alias("days_present"),
        
        # KPI 3: Attendance Percentage
        F.round((F.sum("status") / F.count("date")) * 100, 2).alias("attendance_pct")
    )
)

# Apply Business Logic Filters (e.g., Flag students with low attendance)
df_gold_final = df_gold.withColumn(
    "risk_flag", 
    F.when(F.col("attendance_pct") < 80, "At Risk").otherwise("On Track")
)

# Write to Gold
df_gold_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_gold)

display(df_gold_final)