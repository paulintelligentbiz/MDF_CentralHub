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

# MARKDOWN ********************

# # Bronze GENERIC
# 
# Bronze GENERIC serves as the standardized processing framework for ingesting data from various sources into the Bronze layer. It orchestrates the execution of source-specific API processing while maintaining a consistent approach to data handling, storage, and metadata management.

# MARKDOWN ********************

# ## Parameters
# - Parameters will be provided by the Bronze Pipeline during the ETL process
# - Provide default values for testing purposes

# CELL ********************

source_name = "altmetric"         #"dimensions", "talkwalker_engagements"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Global Notebooks
# Import standard functions and configurations

# CELL ********************

%run Global Imports

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Global Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Start Logging

# CELL ********************

start_time = time.time()
logging.info(f"START Bronze GENERIC processing for {source_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## append_bronze_table

# CELL ********************

@log_process_metrics
def append_bronze_table(
    *,
    view_name: str,
    topic_name: str,
    source_name: str,
    # -- Synapse - base_path pointed the write at the bronze Delta *folder*
    # base_path: str
):
    """Append a bronze table from a global temp view.

    Args:
        view_name: Name of global temp view to read from
        topic_name: Name of current topic
        source_name: Name of data source
        # -- Synapse - base_path: Base path for bronze tables  (removed for Fabric)
    """
    # -- Synapse - "database" is Spark's synonym for schema
    # spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    # -- Fabric - ensure the bronze schema exists in the lakehouse
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")

    # -- Synapse - table name (literal schema)
    # table_name = f"bronze.{view_name}"

    # -- Fabric - table name via the BRONZE schema variable from Global Imports
    table_name = f"{BRONZE}.{view_name}"

    # Read and partition data
    logging.info(f"Reading processed data from global_temp.{view_name}")
    df_partitioned = (spark.table(f"global_temp.{view_name}")
        .select(
            "*",
            F.lit(topic_name).alias("topic_part"),
            F.lit(year).cast('integer').alias("year_part"),
            F.lit(month).cast('integer').alias("month_part"),
            F.lit(day).cast('integer').alias("day_part")
        )
    )

    # Save table
    logging.info(f"Writing to bronze table {table_name}")

    # -- Synapse - external PARQUET table anchored to an ADLS path
    # (df_partitioned.write
    #     .format("parquet")
    #     .mode("append")
    #     .partitionBy("topic_part", "year_part", "month_part", "day_part")
    #     .option("path", f"{base_path}/{view_name}")
    #     .saveAsTable(table_name)
    # )

    # -- Fabric - managed DELTA table in the lakehouse (no path; Delta, not parquet)
    (df_partitioned.write
        .format("delta")
        .mode("append")
        .partitionBy("topic_part", "year_part", "month_part", "day_part")
        .saveAsTable(table_name)
    )

    # Drop the temp view
    logging.info(f"Dropping view global_temp.{view_name}")
    spark.sql(f"DROP VIEW IF EXISTS global_temp.{view_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get Sources and Topics Configs

# CELL ********************

logging.info("Getting source configurations")
source_config, topics_df = get_source_metadata(source_name)
active_topics_count = (topics_df
    .filter(topics_df.Is_Active)
    .count()
)
logging.info(f"Found {active_topics_count} topics to process")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(topics_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Process API Topics

# CELL ********************

# Track offset updates
offset_updates = []

# Process each topic
for topic in topics_df.collect():

    if not topic.Is_Active:
        continue

    topic_name = topic.Topic_Name
    logging.info(f"Processing topic: {topic_name}")

    try:
        # Call API-specific notebook
        # -- Synapse - mssparkutils
        # new_offset = mssparkutils.notebook.run(
        #     source_config.API_Notebook,
        #     1800,
        #     {
        #         "source_name": source_name,
        #         "base_url": source_config.Base_URL,
        #         "topic_name": topic_name,
        #         "project_id": topic.Project_ID,
        #         "search_query": topic.Search_Query,
        #         "countries": topic.Countries,
        #         "resume_offset": topic.Resume_Offset,
        #         "secret_name": source_config.Secret_Name
        #     }
        # )
        # -- Fabric - notebookutils
        new_offset = notebookutils.notebook.run(
            source_config.API_Notebook,
            1800,
            {
                "source_name": source_name,
                "base_url": source_config.Base_URL,
                "topic_name": topic_name,
                "project_id": topic.Project_ID,
                "search_query": topic.Search_Query,
                "countries": topic.Countries,
                "resume_offset": topic.Resume_Offset,
                "secret_name": source_config.Secret_Name
            }
        )

        # Get list of temp views that were created
        if source_name == "dimensions":
            views = [row.viewName for row in spark.sql("SHOW VIEWS IN global_temp").collect()
                    if row.viewName.startswith(f"{source_name}_")]
        else:
            views = [row.viewName for row in spark.sql("SHOW VIEWS IN global_temp").collect()
                    if row.viewName == source_name]

        # Early return if no data to process
        if not views:
            logging.info(f"No data to save for topic {topic_name} - keeping original offset")
            offset_updates.append((topic_name, topic.Resume_Offset))
            continue

        # Append bronze tables
        for view_name in views:
            append_bronze_table(
                view_name=view_name,
                topic_name=topic_name,
                source_name=source_name,
                # -- Synapse - base_path=bronze_Path
                # -- Fabric - base_path removed (managed table, no path)
            )

        offset_updates.append((topic_name, new_offset))

        logging.info(f"Completed processing topic: {topic_name}")
        logging.info(f"New offset: {new_offset}")
    except Exception as e:
        logging.error(f"Failed to process {topic_name}: {type(e).__name__} - {str(e)}")
        # Return original offset on error
        offset_updates.append((topic_name, topic.Resume_Offset))
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## End Logging

# CELL ********************

logging.info("END Bronze GENERIC processing")
log_process_metrics(
    source_name=source_name,
    topic_name="all",
    process_name="Bronze GENERIC",
    status="SUCCESS",
    duration_seconds=int(time.time() - start_time)
)

# -- Synapse - mssparkutils
# mssparkutils.notebook.exit(offset_updates)
# -- Fabric - notebookutils
notebookutils.notebook.exit(offset_updates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
