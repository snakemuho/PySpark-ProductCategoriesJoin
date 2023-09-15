# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 20:10:50 2023

Original file is located at
    https://colab.research.google.com/drive/1jwuaQmB6Cgsani21oI5rl7suOGuA6jgR
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better

# Метод, в который передаются датафреймы продуктов, категорий и их связи
def join_product_and_category(product, category, product_category):
  return product.join(product_category, product["id"]==product_category["product_id"], "left")\
  .select(col("name").alias("Имя продукта"), "category_id")\
  .join(category, category["id"]==product_category["category_id"], "left")\
  .select("Имя продукта", col("name").alias("Имя категории"))

# Пример использования

# Создаём датафреймы из .csv файлов
product_df = spark.read.csv("product.csv", header = True, inferSchema=True)
category_df = spark.read.csv("category.csv", header = True, inferSchema=True)
product_category_df = spark.read.csv("product_category.csv", header = True, inferSchema=True)

# Передаём датафреймы в метод
result_df = join_product_and_category(product_df, category_df, product_category_df)
result_df
