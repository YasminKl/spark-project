from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder     .appName("Mini Data Warehouse")     .config("spark.master", "local")     .getOrCreate()

# Lecture des fichiers CSV
sales_df = spark.read.option("header", True).csv("C:/Users/yasmi/OneDrive/Bureau/ISIC/SID/saprk_project/data/raw//sales.csv")
customers_df = spark.read.option("header", True).csv("C:/Users/yasmi/OneDrive/Bureau/ISIC/SID/saprk_project/data/raw/customers.csv")
#employees_df = spark.read.option("header", True).csv("data/raw/employees.csv")

# Check if DataFrames are empty
print("Sales DataFrame Schema:")
sales_df.printSchema()

print("Customers DataFrame Schema:")
customers_df.printSchema()
# Sauvegarder les fichiers dans la couche Bronze
sales_df.write.mode("overwrite").parquet("output/bronze/sales")
customers_df.write.mode("overwrite").parquet("output/bronze/customers")
#employees_df.write.mode("overwrite").parquet("output/bronze/employees")
