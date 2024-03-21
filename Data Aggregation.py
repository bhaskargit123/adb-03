# Databricks notebook source
# DBTITLE 1,Creating Dataframes
dfcat=spark.read.format('parquet').load('/mnt/source/category/')
dfchnl=spark.read.format('parquet').load('/mnt/source/channel/')
dfdate=spark.read.format('parquet').load('/mnt/source/date/')
dfprod=spark.read.format('parquet').load('/mnt/source/product/')
dfsubcat=spark.read.format('parquet').load('/mnt/source/subcategory/')
dffact=spark.read.format('parquet').load('/mnt/source/factsales/')

# COMMAND ----------

# DBTITLE 1,Create Temp Views by using every dataframe
dfcat.createOrReplaceTempView("vCategory")
dfchnl.createOrReplaceTempView("vChannel")
dfdate.createOrReplaceTempView("vDate")
dfprod.createOrReplaceTempView("vProduct")
dfsubcat.createOrReplaceTempView("vSubcategory")
dffact.createOrReplaceTempView("vFactSales")

# COMMAND ----------

# DBTITLE 1,Applyging Data Transformation
finalDf=spark.sql(
    """
        SELECT 
  ProductCategoryName AS Category,
  ProductSubcategoryName AS Subcategory,
  Brand,
  Year,
  Channel,  
  SUM(SalesAmount) AS Sales, 
  SUM(TotalCost)  AS Cost,
  SUM(SalesQuantity) AS Qty
FROM vFactSales f INNER JOIN vProduct p ON f.ProductKey=p.ProductKey
INNER JOIN vSubcategory s ON p.ProductSubcategoryKey= s.ProductSubcategoryKey
INNER JOIN vCategory c ON c.ProductCategoryKey=s.ProductCategoryKey
INNER JOIN vChannel c ON f.ChannelKey=c.ChannelKey
INNER JOIN vDate  d ON replace(substring(d.DateKey, 1, 10), '-', '')=replace(substring(f.DateKey, 1, 10), '-', '')
GROUP BY
Brand,
ProductSubcategoryName,
ProductCategoryName,
Channel,Year
    """
)


# COMMAND ----------

#display(finalDf)

# COMMAND ----------

finalDf.write.format('parquet').mode('overwrite').save("/mnt/target/output/")
