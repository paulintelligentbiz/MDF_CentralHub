# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5e6e605-c114-4d28-b79b-15b0d0196e4a",
# META       "default_lakehouse_name": "lh_Bronze_Proto",
# META       "default_lakehouse_workspace_id": "03539bd8-5c87-4267-b2bc-eefbcf50de5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5e6e605-c114-4d28-b79b-15b0d0196e4a"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

bronze_metadata_sources_schema = StructType([
    StructField("Source_Name", StringType(), True),
    StructField("API_Notebook", StringType(), True),
    StructField("Base_URL", StringType(), True),
    StructField("Secret_Name", StringType(), True),
])

bronze_metadata_topics_schema = StructType([
    StructField("Source_Name", StringType(), True),
    StructField("Topic_Name", StringType(), True),
    StructField("Project_ID", StringType(), True),
    StructField("Search_Query", StringType(), True),
    StructField("Countries", StringType(), True),
    StructField("Resume_Offset", StringType(), True),
    StructField("Is_Active", BooleanType(), True),
    StructField("Last_Publication_Date", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
