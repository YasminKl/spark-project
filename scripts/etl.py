from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Créer une session Spark
spark = SparkSession.builder     .appName("Mini Data Warehouse")     .config("spark.master", "local")     .getOrCreate()

# Lecture des fichiers CSV
sales_df = spark.read.option("header", True).csv("C:/Users/yasmi/OneDrive/Bureau/ISIC/SID/saprk_project/data/raw/sales.csv")
customers_df = spark.read.option("header", True).csv("C:/Users/yasmi/OneDrive/Bureau/ISIC/SID/saprk_project/data/raw/customers.csv")


# Check if DataFrames are empty
print("Sales DataFrame Schema:")
sales_df.printSchema()
sales_df.show()

print("Customers DataFrame Schema:")
customers_df.printSchema()
customers_df.show()

# Sauvegarder les fichiers dans la couche Bronze
sales_df.write.mode("overwrite").parquet("output/bronze/sales")
customers_df.write.mode("overwrite").parquet("output/bronze/customers")


#Renommer colonne OrderID par OrderID0
sales_df = sales_df.withColumnRenamed("OrderID", "OrderID0")

#Creation de copie de OrderID comme OrderID1
sales_df = sales_df.withColumn("OrderID1", sales_df["OrderID0"])

#Affichage des premiers lignes pour verifier
sales_df.show(2)
customers_df.show(2)


# *________* *________* *________* *________*


#Couche SILVER

# *________* *________* *________* *________*

# Nettoyage du DataFrame sales_df
sales_clean_df = sales_df.dropna(subset=["OrderID0", "ProductID", "Quantity", "UnitPrice", "CustomerID"]) \
    .select(
        "OrderID0", 
        "ProductID", 
        "Quantity", 
        "UnitPrice",  # Nom dans les données brutes
        "CustomerID"
    ).withColumnRenamed("UnitPrice", "Price")  # Renommer la colonne en 'Price'

# Convertir les types de données si nécessaire (par exemple, Quantity doit être un entier)
sales_clean_df = sales_clean_df.withColumn("Quantity", sales_clean_df["Quantity"].cast("integer"))

# Exemple de conversion de Price en DoubleType
sales_clean_df = sales_clean_df.withColumn("Price", sales_clean_df["Price"].cast("double"))

# Suppression des doublons
sales_clean_df = sales_clean_df.dropDuplicates()

# Suppression des doublons
sales_clean_df = sales_clean_df.dropDuplicates()

# Nettoyage du DataFrame customers_df
customers_clean_df = customers_df.dropna(subset=["CustomerID", "ContactName", "Phone"]) \
    .select(
        "CustomerID", 
        "ContactName",  # Nom dans les données brutes
        "Phone"    # Vérifiez que cette colonne existe dans vos données
    ).withColumnRenamed("ContactName", "Name")  # Renommer la colonne en 'Name'

# Vérification de la validité des emails (assurez-vous que la colonne existe)
if "Email" in customers_df.columns:
    customers_clean_df = customers_clean_df.filter(F.col("Email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$"))
else:
    print("La colonne 'Email' n'existe pas dans customers_df.")

# Suppression des doublons
customers_clean_df = customers_clean_df.dropDuplicates()

# Sauvegarder les données nettoyées dans la couche Silver
sales_clean_df.write.mode("overwrite").parquet("output/silver/sales_clean")
customers_clean_df.write.mode("overwrite").parquet("output/silver/customers_clean")
