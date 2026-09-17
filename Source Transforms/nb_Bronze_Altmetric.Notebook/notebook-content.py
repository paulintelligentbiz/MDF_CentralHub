# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a3614891-8c2c-4a33-a78d-7dfedea10306",
# META       "default_lakehouse_name": "lh_Bronze",
# META       "default_lakehouse_workspace_id": "22273b40-4352-4cd5-aa68-cfc1d0f100cc",
# META       "known_lakehouses": [
# META         {
# META           "id": "a3614891-8c2c-4a33-a78d-7dfedea10306"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Process Dimensions Data

# MARKDOWN ********************

# ## Provide Required Parameters

# CELL ********************

# Parameters are passed from Bronze GENERIC, default values are for debugging purposes
source_name = "altmetric"
base_url = "https://api.altmetric.com"
secret_name = "kvAltmetricAPIKey"
topic_name = "PVA"
search_query = None
countries = None
resume_offset = "0,0,0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Global Notebooks

# CELL ********************

%run nb_Global_Imports

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_Global_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_Altmetric_Schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## fetch_altmetric_data

# CELL ********************

@F.udf(returnType=StringType())
def fetch_altmetric_data(dim_id: str) -> str:
    """Fetch Altmetric data for a Dimensions publication ID.
    
    Args:
        row: Row containing publication ID
        
    Returns:
        Tuple of (id, json_response)
    """
    url = f"{base_url}/v1/dimensions_publication_id/{dim_id}"
    response = requests.get(url, params={'key': api_key})
    
    if response.status_code == 200:
        return json.dumps(response.json())
    elif response.status_code == 404:
        return json.dumps({"error": response.status_code})
    else:
        return json.dumps({
            "error": response.status_code,
            "attempted_url": url
        })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## get_dimensions_data

# CELL ********************

def get_dimensions_data(topic_name: str, resume_offset: str) -> tuple[DataFrame, str]:
    """Get Dimensions data for a specific topic.
    
    Args:
        topic_name: Name of topic to process
        resume_offset: Current processing offset (year,month,day)
        
    Returns:
        Tuple containing:
        - DataFrame of filtered Dimensions data
        - Updated offset value
    """
    logging.info(f"Getting Dimensions publication data for topic: {topic_name}")

    # Parse resume offset
    resume_year, resume_month, resume_day = resume_offset.split(",")
    logging.info(f"Using offset: year={resume_year}, month={resume_month}, day={resume_day}")
    
    # Get and filter Dimensions data
    df_filtered = (spark.table("bronze.dimensions_pub")
        .where(F.col("topic_part") == topic_name)
        .where(F.col("year_part") >= resume_year)
        .where(F.col("month_part") >= resume_month) 
        .where(F.col("day_part") >= resume_day)
    )

    record_count = df_filtered.count()
    logging.info(f"Found {record_count} Dimensions records after filtering")

    # Exit if no data found
    if not record_count:
        logging.warning("No Dimensions data found")
        # -- Synapse - mssparkutils
        # mssparkutils.notebook.exit(resume_offset)
        # -- Fabric - notebookutils
        notebookutils.notebook.exit(resume_offset)
    
    # Calculate next offset
    update_values = df_filtered.select(
        F.max("year_part"),
        F.max("month_part"), 
        F.max("day_part")
    ).first()
    
    next_offset = ','.join(map(str, update_values))
    
    return df_filtered, next_offset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## process_topic_data

# CELL ********************

@log_process_metrics
def process_topic_data(
    source_name: str,
    topic_name: str,
    base_url: str,
    api_key: str,
    resume_offset: str
) -> str:
    """Process Altmetric data for a specific topic's Dimensions publications.

    Args:
        source_name: Name of the data source ('altmetric')
        topic_name: Name of topic to process (e.g., 'Dioxane', 'PVA')
        base_url: Altmetric API base URL
        api_key: Altmetric API access token
        resume_offset: Processing offset in format 'year,month,day'
        
    Returns:
        Updated resume_offset
    """
    logging.info(f"BEGIN processing topic: {topic_name}")
    logging.info(f"Current resume_offset: {resume_offset}")
    
    # Get filtered Dimensions data
    df_filtered, next_offset = get_dimensions_data(topic_name, resume_offset)
    
    # Check if we have new data
    if next_offset == resume_offset:
        logging.warning("No new data to process")
        # -- Synapse - mssparkutils
        # mssparkutils.notebook.exit(resume_offset)
        # -- Fabric - notebookutils
        notebookutils.notebook.exit(resume_offset)
        
    # Process altmetric results
    df_responses = (df_filtered
        .select("id")
        .distinct()
        .withColumn("response", fetch_altmetric_data("id"))
        .cache()
    )

    # Parse JSON responses
    logging.info("Parsing JSON responses")
    df_parsed = df_responses.select(
        "*",
        F.from_json(F.col("response"), bronze_altmetric_schema).alias("response_dict")
    )
    
    # Explode nested structures
    logging.info("Exploding nested structures")
    df_final = explode_columns_recursive(df_parsed, separator="__")

    # Create temp views for Bronze GENERIC
    logging.info("Creating global temp view")
    df_final.createOrReplaceGlobalTempView(f"{source_name}")
    
    logging.info(f"Next offset: {next_offset}")
    logging.info(f"END processing topic: {topic_name}")

    return next_offset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main

# CELL ********************

# Get API key
# -- Synapse - Key Vault via linked service (3-arg mssparkutils)
# api_key = mssparkutils.credentials.getSecret(keyvault_name, secret_name, linked_service_keyvault)
# -- Fabric - Key Vault by URL via notebookutils
api_key = notebookutils.credentials.getSecret(KEYVAULT_URL, secret_name)

# Process topic data
new_offset = process_topic_data(
    source_name=source_name,
    topic_name=topic_name,
    base_url=base_url,
    api_key=api_key,
    resume_offset=resume_offset
)

# Return updated offset
# -- Synapse - mssparkutils
# mssparkutils.notebook.exit(new_offset)
# -- Fabric - notebookutils
notebookutils.notebook.exit(new_offset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
