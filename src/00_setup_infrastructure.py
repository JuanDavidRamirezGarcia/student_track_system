# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC --1. Create the catalog
# MAGIC CREATE CATALOG IF NOT EXISTS student_system;
# MAGIC
# MAGIC -- 2. Create the Schema 
# MAGIC CREATE SCHEMA IF NOT EXISTS student_system.attendance;
# MAGIC
# MAGIC -- 3. Verify it worked
# MAGIC DESCRIBE SCHEMA student_system.attendance;