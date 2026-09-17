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

# # Reload Bronze Metadata Tables

# MARKDOWN ********************

# ## Source Fields (meta.bronze_sources)
# - **Source_Name**:  Unique identifier 
# - **API_Notebook**: Processing notebook for source
# - **Base_URL**:     API root URL or empty if file-based
# - **Secret_Name**:  Key Vault secret name or empty if no auth
# 
# ## Topic Fields (meta.bronze_topics)
# - **Source_Name**:  Links to source metadata
# - **Topic_Name**:   Topic identifier or folder name if file-based
# - **Search_Query**: Query string or talkwalker collector ID
# - **Talkwalker_Project_Id**:  cid for talkwalker where the query lives
# - **Countries**:    Used by `talkwalker_influencers` for filtering
# - **Resume_Offset**: Next processing starting point
# - **Is_Active**:    Processing control (1=active, 0=skip)
# 
# ### Sources Metadata Summary
# | Source | API_Notebook | Base_URL | Secret_Name |
# |--------|-------------|-----------|-------------|
# | altmetric | Bronze Altmetric | API Base URL | Required |
# | dimensions | Bronze Dimensions | API Base URL | Required |
# | glimpse | Bronze Glimpse | Not used | Not used |
# | polimonitor | Bronze Polimonitor | Not used | Not used |
# | talkwalker | Bronze Talkwalker | API Base URL | Required |
# | talkwalker_engagements | Bronze Talkwalker Engagement | API Base URL | Required |
# | talkwalker_influencers | Bronze Talkwalker Influencers | API Base URL | Required |
# | usgovinfo | Bronze US Gov Info | API Base URL | Required |
# 
# ### Topics Metadata Summary
# | Source | Topic_Name | Search_Query | Talkwalker_Project_Id | Countries | Resume_Offset | Is_Active |
# |--------|------------|--------------|-----------|---------------|-----------|-----------|
# | altmetric | Topic Name | Not used | Not used | Not used | Year, Month, Day | 1 or 0 |
# | dimensions | Topic name | Query string | Not used | Not used | # of records to skip | 1 or 0 |
# | glimpse | Folder/Topic name | Not used | Not used | Not used | File count | 1 or 0 |
# | polimonitor | Folder/Topic Name | Not used | Not used | Not used | File count | 1 or 0 |
# | talkwalker | Topic name | Collector ID | Not used | Not used | "earliest" or epoch ms | 1 or 0 |
# | talkwalker_engagements | Topic name | Search Query ID | Project ID | Not used | epoch ms | 1 or 0 |
# | talkwalker_influencers | Topic name | Search Query ID |  Project ID | Required | epoch ms | 1 or 0 |
# | usgovinfo | Topic name | Query string | Not used | Not used | "*" or offsetMark ID | 1 or 0 |
# 


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

%run nb_Bronze_Metadata_Schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Bronze Database

# CELL ********************

# MAGIC %%sql
# MAGIC -- Synapse: CREATE DATABASE IF NOT EXISTS meta
# MAGIC -- Fabric : CREATE SCHEMA IF NOT EXISTS meta  (schema-enabled lakehouse)
# MAGIC CREATE SCHEMA IF NOT EXISTS meta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Sources Dataframe

# CELL ********************

df_sources = spark.createDataFrame(
    [
        Row(
            Source_Name='altmetric',
            API_Notebook='Bronze Altmetric',
            Base_URL='https://api.altmetric.com',
            Secret_Name='kvAltmetricAPIKey'
        ),
        Row(
            Source_Name='dimensions',
            API_Notebook='Bronze Dimensions',
            Base_URL='https://app.dimensions.ai',
            Secret_Name='kvDimensionsAPIKey'
        ),
        Row(
            Source_Name='glimpse',
            API_Notebook='Bronze Glimpse',
            Base_URL=None,
            Secret_Name=None
        ),
        Row(
            Source_Name='polimonitor',
            API_Notebook='Bronze Polimonitor',
            Base_URL=None,
            Secret_Name=None
        ),
        Row(
            Source_Name='talkwalker',
            API_Notebook='Bronze Talkwalker',
            Base_URL='https://api.talkwalker.com',
            Secret_Name='kvTalkwalkerAccessToken'
        ),
        Row(
            Source_Name='talkwalker_engagements',
            API_Notebook='Bronze Talkwalker Engagements', 
            Base_URL='https://api.talkwalker.com',
            Secret_Name='kvTalkwalkerAccessToken'
        ),
        Row(
            Source_Name='talkwalker_influencers',
            API_Notebook='Bronze Talkwalker Influencers',
            Base_URL='https://api.talkwalker.com',
            Secret_Name='kvTalkwalkerAccessToken'
        ),
        Row(
            Source_Name='usgovinfo',
            API_Notebook='Bronze US Gov Info',
            Base_URL='https://api.govinfo.gov',
            Secret_Name='kvUSGovInfo'
        ),
        Row(
            Source_Name='youscan',
            API_Notebook='Bronze YouScan',
            Base_URL='https://api.youscan.io',
            Secret_Name='kvYouScanAPIKey'
        )
    ],
    schema=bronze_metadata_sources_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Topics Dataframe

# CELL ********************

# -- Fabric: DISABLED - stale df_topics version (missing Project_ID).
# -- The authoritative topics list is the '12.19.2025 with project ids' cell below.
# df_topics = spark.createDataFrame(
#     [
#         Row(
#             Source_Name='altmetric',
#             Topic_Name='Dioxane',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset="0,0,0",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='altmetric',
#             Topic_Name='Packaging Waste',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset="0,0,0",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='altmetric',
#             Topic_Name='PVA',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset="0,0,0",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='altmetric',
#             Topic_Name='Unions',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset="0,0,0",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='dimensions',
#             Topic_Name='Dioxane',
#             Search_Query=(
#                 'dioxane '
#                 'OR "diethylene dioxide" '
#                 'OR "diethylene ether" '
#                 'OR "ethylene dioxane" '
#                 'OR "solvent stabilizer" '
#                 'OR "solvent stabiliser"'
#             ),
#             Countries=None,
#             # Resume_Offset="0",
#             Resume_Offset="24000", # faster testing
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='dimensions',
#             Topic_Name='PVA',
#             Search_Query=(
#                 'PVOH '
#                 'OR PVA '
#                 'OR "polyvinyl alcohol" '
#                 'OR (PVAL AND (chemical* OR compound* OR substance OR molecule*)) '
#                 'OR polyethenol '
#                 'OR "alcoholysis resin" '
#                 'OR "alcohol-soluble resin" '
#                 'OR "ethanol homopolymer" '
#                 'OR elvanol '
#                 'OR vinol '
#                 'OR gelvatol'
#             ),
#             Countries=None,
#             # Resume_Offset="0",
#             Resume_Offset="49000", # faster testing
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='dimensions',
#             Topic_Name='Packaging Waste',
#             Search_Query=(
#                 '('
#                     '('
#                         '(packaging AND waste)'
#                         ' OR ('
#                             '('
#                                 'ERP'
#                                 ' OR minimi?ation'
#                                 ' OR LCA'
#                                 ' OR "Life Cycle Assessment"'
#                                 ' OR "producer responsibility"'
#                                 ' OR "responsibility of producer"'
#                                 ' OR "extended responsibility"'
#                             ')'
#                             ' AND '
#                             '(packaging OR waste)'
#                         ')'
#                     ')'
#                     ' NOT ('
#                         '(ERP AND ('
#                             '"enterprise resource planning"'
#                             ' OR "effective radiated power"'
#                             ' OR "electronic Road Pricing"'
#                             ' OR "estimated retail price"'
#                             ' OR "engine power reduction"'
#                             ' OR "event processing rule"'
#                             ' OR "electronic patient record"'
#                         '))'
#                         ' OR "Labor Condition Application"'
#                         ' OR "London City Airport"'
#                         ' OR "Assualt"'
#                     ')'
#                 ')'
#             ),
#             Countries=None,
#             # Resume_Offset="0",
#             Resume_Offset="29000", # faster testing
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='dimensions',
#             Topic_Name='Unions',
#             Search_Query=(
#                 '"unions workers"~5 '
#                 'OR "unionisation workers"~5'
#             ),
#             Countries=None,
#             # Resume_Offset="0",
#             Resume_Offset="7000", # faster testing
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='glimpse',
#             Topic_Name='Dioxane',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='glimpse',
#             Topic_Name='Packaging Waste',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='glimpse',
#             Topic_Name='Unions',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='polimonitor',
#             Topic_Name='Dioxane',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='polimonitor',
#             Topic_Name='PVA',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='polimonitor',
#             Topic_Name='Packaging Waste',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='polimonitor',
#             Topic_Name='Unions',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker',
#             Topic_Name='combined',
#             Search_Query='project_lakehouse_demo',
#             Countries=None,
#             Resume_Offset='earliest',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_engagements',
#             Topic_Name='Dioxane',
#             Search_Query='lt5yyjmb_donwf3pzwepy',
#             Countries=None,
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_engagements',
#             Topic_Name='PVA',
#             Search_Query='lxm3hhvz_donwf3o2ag9p',
#             Countries=None,
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_engagements',
#             Topic_Name='Packaging Waste',
#             Search_Query='lsyrprzr_donwf34y7kga',
#             Countries=None,
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_engagements',
#             Topic_Name='Unions',
#             Search_Query='lt5yw0za_donwf3pzxoq9',
#             Countries=None,
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_influencers',
#             Topic_Name='Dioxane',
#             Search_Query='lt5yyjmb_donwf3pzwepy',
#             Countries='["us", "uk", "de"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_influencers',
#             Topic_Name='PVA',
#             Search_Query='lxm3hhvz_donwf3o2ag9p',
#             Countries='["us", "uk", "de"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_influencers',
#             Topic_Name='Packaging Waste',
#             Search_Query='lsyrprzr_donwf34y7kga',
#             Countries='["us", "uk", "de"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_influencers',
#             Topic_Name='Unions',
#             Search_Query='lt5yw0za_donwf3pzxoq9',
#             Countries='["us", "uk", "de"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='usgovinfo',
#             Topic_Name='Dioxane',
#             Search_Query=(
#                 'dioxane '
#                 'OR "diethylene dioxide" '
#                 'OR "diethylene ether" '
#                 'OR "ethylene dioxane" '
#                 'OR "solvent stabilizer" '
#                 'OR "solvent stabiliser"'
#             ),
#             Countries=None,
#             Resume_Offset="*",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='usgovinfo',
#             Topic_Name='PVA',
#             Search_Query=(
#                 'PVOH '
#                 'OR PVA '
#                 'OR "polyvinyl alcohol" '
#                 'OR (PVAL AND (chemical* OR compound* OR substance OR molecule*)) '
#                 'OR polyethenol '
#                 'OR "alcoholysis resin" '
#                 'OR "alcohol-soluble resin" '
#                 'OR "ethanol homopolymer" '
#                 'OR elvanol '
#                 'OR vinol '
#                 'OR gelvatol'
#             ),
#             Countries=None,
#             Resume_Offset="*",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='usgovinfo',
#             Topic_Name='Packaging Waste',
#             Search_Query=(
#                 '('
#                     '('
#                         '(packaging AND waste)'
#                         ' OR ('
#                             '('
#                                 'ERP'
#                                 ' OR minimization'
#                                 ' OR LCA'
#                                 ' OR "Life Cycle Assessment"'
#                                 ' OR "producer responsibility"'
#                                 ' OR "responsibility of producer"'
#                                 ' OR "extended responsibility"'
#                             ')'
#                             ' AND '
#                             '(packaging OR waste)'
#                         ')'
#                     ')'
#                     ' NOT ('
#                         '(ERP AND ('
#                             '"enterprise resource planning"'
#                             ' OR "effective radiated power"'
#                             ' OR "electronic Road Pricing"'
#                             ' OR "estimated retail price"'
#                             ' OR "engine power reduction"'
#                             ' OR "event processing rule"'
#                             ' OR "electronic patient record"'
#                         '))'
#                         ' OR "Labor Condition Application"'
#                         ' OR "London City Airport"'
#                         ' OR "Assualt"'
#                     ')'
#                 ')'
#             ),
#             Countries=None,
#             Resume_Offset="*",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='usgovinfo',
#             Topic_Name='Unions',
#             Search_Query=(
#                 '(unions NEAR/5 workers) '
#                 'OR (unionisation NEAR/5 workers)'
#             ),
#             Countries=None,
#             Resume_Offset="*",
#             Is_Active=True
#         )
#     ],
#     schema=bronze_metadata_topics_schema
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_topics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- Fabric: DISABLED - stale df_topics version (missing Project_ID).
# -- The authoritative topics list is the '12.19.2025 with project ids' cell below.
# df_topics = spark.createDataFrame(
#     [
#         Row(
#             Source_Name='altmetric',
#             Topic_Name='Urban Waste Water',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset="0,0,0",
#             Is_Active=True
#         ),
# 		Row(
#             Source_Name='dimensions',
#             Topic_Name='Urban Waste Water',
#             Search_Query='(("urban wastewater treatment" OR "municipal wastewater treatment" OR "sewage treatment" OR "wastewater purification" OR "advanced wastewater treatment" OR "quaternary treatment" OR "fourth stage treatment" OR "traitement des eaux usées urbaines" OR "traitement des eaux usées municipales" OR "traitement des eaux usées" OR "épuration des eaux usées" OR "traitement quaternaire" OR "traitement avancé des eaux usées" OR "kommunale Abwasserbehandlung" OR "städtische Abwasserbehandlung" OR "Abwasserreinigung" OR "Abwasseraufbereitung" OR "quaternäre Behandlung" OR "vierte Stufe der Abwasserbehandlung" OR  "trattamento delle acque reflue urbane" OR "trattamento delle acque reflue municipali" OR "trattamento delle acque di scarico" OR "depurazione delle acque reflue" OR "trattamento quaternario" OR "trattamento avanzato delle acque reflue") AND (micropollu* OR micropolveri OR Mikroschadstoffe OR "micro-pollutants" OR "micro pollutants" OR "emerging contaminants" OR "emerging pollutants" OR "micropolluants" OR "polluants émergents" OR "microcontaminants" OR "Mikroschadstoffe" OR "Spurenstoffe" OR "neuartige Schadstoffe" OR "microinquinanti" OR "contaminanti emergenti" OR "micropolveri inquinanti") NOT (stormwater OR "storm water" OR Overflows OR POME OR "Palm Oil" OR rivers))',
#             Countries=None,
#             Resume_Offset="0",
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='glimpse',
#             Topic_Name='Urban Waste Water',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='polimonitor',
#             Topic_Name='Urban Waste Water',
#             Search_Query=None,
#             Countries=None,
#             Resume_Offset='0',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker',
#             Topic_Name='combined',
#             Search_Query='urbanwastewater',
#             Countries=None,
#             Resume_Offset='earliest',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_engagements',
#             Topic_Name='Urban Waste Water',
#             Search_Query='m9gxs13v_dnux9gi2h2dw',
#             Countries='["uk", "de", "fr", "it", "pl", "es"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='talkwalker_influencers',
#             Topic_Name='Urban Waste Water',
#             Search_Query='m9gxs13v_dnux9gi2h2dw',
#             Countries='["uk", "de", "fr", "it", "pl", "es"]',
#             Resume_Offset='1645574400000',
#             Is_Active=True
#         ),
#         Row(
#             Source_Name='youscan',
#             Topic_Name='Urban Waste Water',
#             Search_Query='402855',
#             Countries=None,
#             Resume_Offset="0",
#             Is_Active=True
#         )
#         # Row(
#         #     Source_Name='usgovinfo',
#         #     Topic_Name='Urban Waste Water',
#         #     Search_Query=(
#         #         '('
#         #             '('
#         #                 'micropollu* '
#         #                 'AND '
#         #                 '('
#         #                     '"waste water" '
#         #                     'OR "wastewater" '
#         #                     'OR "EPR" '
#         #                     'OR "extended producer responsibility" '
#         #                     'OR cosmetic*'
#         #                 ') '
#         #                 'NOT ("Electron Paramagnetic Resonance")'
#         #             ') '
#         #             'OR ("european urban wastewater treatment directive") '
#         #             'OR ('
#         #                 '("waste water" OR "wastewater") '
#         #                 'AND ('
#         #                     '"EPR" '
#         #                     'OR "extended producer responsibility" '
#         #                     'OR "cosmetics" '
#         #                     'OR "biodegradability" '
#         #                     'OR "eutrophication" '
#         #                     'OR "water quality" '
#         #                     'OR "microplastics"'
#         #                 ') '
#         #                 'NOT ("Electron Paramagnetic Resonance")'
#         #             ')'
#         #         ')'
#         #     ),
#         #     Countries=None,
#         #     Resume_Offset="*",
#         #     Is_Active=True
#         # ),
#     ],
#     schema=bronze_metadata_topics_schema
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Topics as of 12.19.2025 with project ids
df_topics = spark.createDataFrame(
    [
        Row(
            Source_Name='glimpse',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Food Quality & Safety',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw2a2o_12dqqlwa2fkdi',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Animal Welfare',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((cow OR cows OR calf OR calves) AND (separation OR "colostrum deprivation" OR "animal welfare" OR "factory farming" OR cruelty OR abuse OR neglect OR dehorn OR "tail dock" OR overmilk OR mastitis OR "artificial insemination")) OR "free the cows") AND NOT (rodeo OR "calf roping" OR "steer wrestling" OR bulldogging OR elephant OR bull OR bulls)',
            Countries=None,
            Resume_Offset='AoJw4JnVmJsDOEZSLTIwMjUtMTItMTgtMjAyNS0yMzI2OA==',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Health & Wellness',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw9liq_12dqqlwa2f25t',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Dairy Alternatives & Substitutions',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw4m0k_12dqqlwa2f3ov',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='polimonitor',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='7',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Activism',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '(("Direct Action Everywhere"~20) OR ("DxE"~20) OR ("Mercy for Animals"~20) OR ("People for the Ethical Treatment of Animals"~20) OR ("PETA"~20) OR ("Physicians Committee for Responsible Medicine"~20) OR ("PCRM"~20) OR ("Center for Science in the Public Interest"~20) OR ("CSPI"~20) OR ("Environmental Working Group"~20) OR ("EWG"~20) OR ("Humane Society of the United States"~20) OR ("HSUS"~20) OR ("Switch4Good"~20) OR ("Switch 4 Good"~20) OR ("Our Honor"~20) OR ("Crystal Heath"~20)) AND (("dairy"~20) OR ("cow"~20) OR ("cows"~20) OR ("calf"~20) OR ("calves"~20) OR ("milk"~20) OR ("milks"~20) OR ("milked"~20) OR ("milking"~20) OR ("milkshake"~20) OR ("yogurt"~20) OR ("yoghurt"~20) OR ("cream"~20) OR ("creams"~20) OR ("kefir"~20) OR ("cheese"~20) OR ("cheeses"~20) OR ("butter"~20) OR ("butters"~20) OR ("buttermilk"~20) OR ("whey"~20) OR ("casein"~20) OR ("lactose"~20) OR ("factory farms"~20))'
            ),
            Countries=None,
            Resume_Offset='85',
            Is_Active=True,
            Last_Publication_Date='2025-10-28'
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Animal Welfare',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyvu8re_12dqqlwa2eba1',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Dairy Alternatives & Substitutions',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='("non-dairy" OR nondairy OR ((vegan OR alternative OR substitut) NEAR/2 (milk OR cream OR butter OR yogurt OR yoghurt OR kefir OR cheese OR custard))) AND NOT ("donkey milk" OR "donkey soap" OR "cockroach milk" OR "market report" OR forecast OR "market update" OR CAGR OR "market size" OR "forecasts")',
            Countries=None,
            Resume_Offset='AoJwoM6LlpsDMEJJTExTLTExOXMyMjJlbnI=',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Food Quality & Safety',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("dairy bird flu"~20) OR ("dairies bird flu"~20) OR ("cow bird flu"~20) OR ("cows bird flu"~20) OR ("calf bird flu"~20) OR ("calves bird flu"~20) OR ("milk bird flu"~20) OR ("milks bird flu"~20) OR ("milked bird flu"~20) OR ("milking bird flu"~20) OR ("milkshake bird flu"~20) OR ("milkshakes bird flu"~20) OR ("yogurt bird flu"~20) OR ("yoghurts bird flu"~20) OR ("cream bird flu"~20) OR ("cheese bird flu"~20) OR ("cheeses bird flu"~20) OR ("butter bird flu"~20) OR ("butters bird flu"~20) OR ("buttermilk bird flu"~20) OR ("buttermilks bird flu"~20) OR ("whey bird flu"~20) OR ("casein bird flu"~20) OR ("lactose bird flu"~20) OR ("kefir bird flu"~20) OR ("kefirs bird flu"~20) OR ("dairy avian flu"~20) OR ("cow avian influenza"~20) OR ("milk H5N1"~20) OR ("cheese HPAI"~20) OR ("cows H5N1"~20) OR ("calves avian influenza"~20) OR ("whey bird flu"~20) OR ("milk HPAI"~20)'
            ),
            Countries=None,
            Resume_Offset='290',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit06pta_dnux9mauizd3',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit063sp_dnux9mauiruq',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Farming, Marketplace & Industry',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("dairy industry"~3) OR ("dairy farms"~3) OR ("dairy farmers"~3) OR ("dairy farm operations"~3) OR ("dairy operations"~3) OR ("dairy and cattle farmers"~3) OR ("American Dairy Association of Indiana") OR ("American Dairy Association Mideast") OR ("American Dairy Association North East") OR ("California Milk Advisory Board") OR ("The Dairy Alliance") OR ("Dairy Farmers of Washington") OR ("Dairy Farmers of Wisconsin") OR ("Dairy Management West") OR ("Arizona Milk Producers") OR ("Dairy MAX") OR ("Dairy West") OR ("Florida Dairy Farmers") OR ("Maine Dairy Promotion") OR ("Midwest Dairy") OR ("New England Dairy") OR ("Oregon Dairy and Nutrition Council") OR ("United Dairy Industry of Michigan") OR ("National Dairy Council") OR ("American Dairy Association") OR ("Dairy Management Inc") OR ("Dairy Management Incorporated") OR ("National Milk Producers Federation") OR ("dairy CMAB"~10) OR ("cow CMAB"~10) OR ("milk CMAB"~10) OR ("cheese CMAB"~10) OR ("yogurt CMAB"~10) OR ("butter CMAB"~10) OR ("whey CMAB"~10) OR ("casein CMAB"~10) OR ("lactose CMAB"~10) OR ("dairy NDC"~10) OR ("cow NDC"~10) OR ("milk NDC"~10) OR ("cheese NDC"~10) OR ("yogurt NDC"~10) OR ("butter NDC"~10) OR ("whey NDC"~10) OR ("casein NDC"~10) OR ("lactose NDC"~10) OR ("dairy NMPF"~10) OR ("cow NMPF"~10) OR ("milk NMPF"~10) OR ("cheese NMPF"~10) OR ("yogurt NMPF"~10) OR ("butter NMPF"~10) OR ("whey NMPF"~10) OR ("casein NMPF"~10) OR ("lactose NMPF"~10) OR ("labor dairy"~10 AND migrant) OR ("labor dairy"~10 AND immigrant) OR ("labor dairy"~10 AND documented) OR ("labor dairy"~10 AND undocumented) OR ("labor dairy"~10 AND illegal) OR ("laborer dairy"~10 AND migrant) OR ("laborer dairy"~10 AND immigrant) OR ("laborer dairy"~10 AND documented) OR ("laborer dairy"~10 AND undocumented) OR ("laborer dairy"~10 AND illegal) OR ("worker dairy"~10 AND migrant) OR ("worker dairy"~10 AND immigrant) OR ("worker dairy"~10 AND documented) OR ("worker dairy"~10 AND undocumented) OR ("worker dairy"~10 AND illegal) OR ("workers dairy"~10 AND migrant) OR ("workers dairy"~10 AND immigrant) OR ("workers dairy"~10 AND documented) OR ("workers dairy"~10 AND undocumented) OR ("workers dairy"~10 AND illegal) OR ("farmhand dairy"~10 AND migrant) OR ("farmhand dairy"~10 AND immigrant) OR ("farmhand dairy"~10 AND documented) OR ("farmhand dairy"~10 AND undocumented) OR ("farmhand dairy"~10 AND illegal) OR ("farm workforce modernization act") OR ("H.R.1603") OR ("H.R. 1603") OR ("HR 1603") OR ("HR1603") OR ("dairy H2A"~10) OR ("dairy H-2A"~10) OR ("dairy visa"~10)'
            ),
            Countries=None,
            Resume_Offset='40562',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='glimpse',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Dairy Alternatives & Substitutions',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw4m0k_12dqqlwa2f3ov',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Sustainability',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("dairy sustainability"~20) OR ("dairy sustainable"~20) OR ("dairy ecofriendly"~20) OR ("dairy eco-friendly"~20) OR ("dairy greenhouse gas"~20) OR ("dairy greenhouse gases"~20) OR ("dairy GHG"~20) OR ("dairy climate change"~20) OR ("dairy climate friendly"~20) OR ("dairy methane"~20) OR ("dairy environmental impact"~20) OR ("dairy net zero"~20) OR ("dairy carbon emissions"~20) OR ("dairy emissions"~20) OR ("dairy carbon footprint"~20) OR ("dairy conservation"~20) OR ("dairy greenwash*"~20) OR ("dairy carbon offset"~20) OR ("dairy renewable energy"~20) OR ("dairy biodiversity"~20) OR ("dairy carbon neutral"~20) OR ("dairy carbon neutrality"~20) OR ("dairy pollut*"~20) OR ("cow sustainability"~20) OR ("cow sustainable"~20) OR ("cow ecofriendly"~20) OR ("cow eco-friendly"~20) OR ("cow greenhouse gas"~20) OR ("cow greenhouse gases"~20) OR ("cow GHG"~20) OR ("cow climate change"~20) OR ("cow climate friendly"~20) OR ("cow methane"~20) OR ("cow environmental impact"~20) OR ("cow net zero"~20) OR ("cow carbon emissions"~20) OR ("cow emissions"~20) OR ("cow carbon footprint"~20) OR ("cow conservation"~20) OR ("cow greenwash*"~20) OR ("cow carbon offset"~20) OR ("cow renewable energy"~20) OR ("cow biodiversity"~20) OR ("cow carbon neutral"~20) OR ("cow carbon neutrality"~20) OR ("cow pollut*"~20) OR ("cows sustainability"~20) OR ("cows sustainable"~20) OR ("cows ecofriendly"~20) OR ("cows eco-friendly"~20) OR ("cows greenhouse gas"~20) OR ("cows greenhouse gases"~20) OR ("cows GHG"~20) OR ("cows climate change"~20) OR ("cows climate friendly"~20) OR ("cows methane"~20) OR ("cows environmental impact"~20) OR ("cows net zero"~20) OR ("cows carbon emissions"~20) OR ("cows emissions"~20) OR ("cows carbon footprint"~20) OR ("cows conservation"~20) OR ("cows greenwash*"~20) OR ("cows carbon offset"~20) OR ("cows renewable energy"~20) OR ("cows biodiversity"~20) OR ("cows carbon neutral"~20) OR ("cows carbon neutrality"~20) OR ("cows pollut*"~20) OR ("milk sustainability"~20) OR ("milks sustainability"~20) OR ("milked sustainability"~20) OR ("milking sustainability"~20) OR ("milkshake* sustainability"~20) OR ("yogurt* sustainability"~20) OR ("yoghurt* sustainability"~20) OR ("cream sustainability"~20) OR ("creams sustainability"~20) OR ("kefir* sustainability"~20) OR ("cheese sustainability"~20) OR ("cheeses sustainability"~20) OR ("butter sustainability"~20) OR ("butters sustainability"~20) OR ("buttermilk* sustainability"~20) OR ("whey sustainability"~20) OR ("casein sustainability"~20) OR ("lactose sustainability"~20) OR ("bovaer sustainability"~20)'
            ),
            Countries=None,
            Resume_Offset='13329',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='glimpse',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Animal Welfare',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyvu8re_12dqqlwa2eba1',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Health & Wellness',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420115',
            Countries='["us"]',
            Resume_Offset='1241499',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='445452',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='7110',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Health & Wellness',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw9liq_12dqqlwa2f25t',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Sustainability',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywhoi4_12dqqlwa2e6ga',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Farming, Marketplace & Industry',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywdya4_12dqqlwa2ehac',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Policy & Regulation',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywg6eo_12dqqlwa2f5x6',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='polimonitor',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='7',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit083lu_dnux9maujem1',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Policy & Regulation',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420133',
            Countries='["us"]',
            Resume_Offset='96003',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Food Quality & Safety',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyw2a2o_12dqqlwa2fkdi',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='glimpse',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Farming, Marketplace & Industry',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((dairy OR milk OR yogurt OR cheese OR butter OR whey OR casein OR lactose) NEAR/25 (stereotype OR myth OR cancer OR carcinogen OR mucus OR disease OR acne OR inflammation OR health OR nutrition OR diabetes OR USDA OR guidelines))',
            Countries=None,
            Resume_Offset='AoJwoOWem5sDPxVDQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1DQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1wdDEz',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit05bf8_dnux9mauir0p',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Activism',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((cow AND (protest OR petition OR campaign OR rally OR demonstration OR boycott OR advocacy OR "animal rights" OR grassroots OR "call to action" OR "colostrum deprivation" OR "factory farming" OR dehorn OR "tail dock" OR overmilk OR mastitis OR "artificial insemination")) OR "free the cows") AND NOT (rodeo OR "calf roping" OR "steer wrestling" OR bulldogging OR elephant* OR bull)',
            Countries=None,
            Resume_Offset='AoJwoM6LlpsDPw5DUkVDLTIwMjUtMTItMTctQ1JFQy0yMDI1LTEyLTE3LXB0MS1QZ0UxMjIxLTQ=',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=(
                '(retinoid OR retinol OR "retinoic acid" OR retinaldehyde OR tretinoin OR isotretinoin OR adapalene OR tazarotene OR "retinyl palmitate" OR "retinyl acetate" OR "retinyl propionate" OR "retinyl linoleate" OR "retinyl retinoate" OR "hydroxypinacolone retinoate" OR "granactive retinoid" OR "Retin-A" OR Differin OR Accutane OR trifarotene OR rétinoïde* OR rétinol OR "acide rétinoïque" OR rétinaldéhyde OR trétinoïne OR isotrétinoïne OR adapalène OR tazarothène OR "palmitate de rétinyl" OR "acétate de rétinyl" OR "propionate de rétinyl" OR "linolate de rétinyl" OR "rétinoate de rétinyl" OR "rétinoate d\'hydroxypinacolone" OR "rétinoïde Granactive" OR "pro-rétinoïde" OR "Retin-A" OR Differin OR Accutane OR trifarothène OR Retinoid* OR Retinol OR "Retinsäure" OR Retinaldehyd OR Tretinoin OR Isotretinoin OR Adapalen OR Tazaroten OR "Retinylpalmitat" OR "Retinylacetat" OR "Retinylpropionat" OR "Retinyllinoleat" OR "Retinylretinoat" OR "hydroxypinakoleines Retinoat" OR "Granactive Retinoid" OR "Pro-Retinoid" OR "Retin-A" OR Differin OR Accutane OR Trifaretin)'
            ),
            Countries=None,
            Resume_Offset='50000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Farming, Marketplace & Industry',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywdya4_12dqqlwa2ehac',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Health & Wellness',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("dairy health"~10) OR ("dairy immunity"~10) OR ("dairy weight loss"~10) OR ("dairy weight management"~10) OR ("dairy nutrition"~10) OR ("dairy nutritious"~10) OR ("dairy nutrients"~10) OR ("dairy protein"~10) OR ("dairy calcium"~10) OR ("dairy vitamin D"~10) OR ("dairy vitamins"~10) OR ("dairy minerals"~10) OR ("dairy iodine"~10) OR ("dairy vitamin B12"~10) OR ("dairy zinc"~10) OR ("dairy selenium"~10) OR ("dairy niacin"~10) OR ("dairy riboflavin"~10) OR ("dairy probiotics"~10) OR ("dairy vitamin A"~10) OR ("dairy potassium"~10) OR ("dairy brain development"~10) OR ("dairy energy"~10) OR ("dairy metabolism"~10) OR ("dairy blood pressure"~10) OR ("dairy diabetes"~10) OR ("dairy glucose"~10) OR ("dairy healthy"~10) OR ("milk health"~10) OR ("milk immunity"~10) OR ("milk weight loss"~10) OR ("milk weight management"~10) OR ("milk nutrition"~10) OR ("milk nutritious"~10) OR ("milk nutrients"~10) OR ("milk protein"~10) OR ("milk calcium"~10) OR ("milk vitamin D"~10) OR ("milk vitamins"~10) OR ("milk minerals"~10) OR ("milk iodine"~10) OR ("milk vitamin B12"~10) OR ("milk zinc"~10) OR ("milk selenium"~10) OR ("milk niacin"~10) OR ("milk riboflavin"~10) OR ("milk probiotics"~10) OR ("milk vitamin A"~10) OR ("milk potassium"~10) OR ("milk brain development"~10) OR ("milk energy"~10) OR ("milk metabolism"~10) OR ("milk blood pressure"~10) OR ("milk diabetes"~10) OR ("milk glucose"~10) OR ("milk healthy"~10) OR ("milks health"~10) OR ("milks immunity"~10) OR ("milks weight loss"~10) OR ("milks protein"~10) OR ("milks calcium"~10) OR ("milks vitamin D"~10) OR ("milks probiotics"~10) OR ("milks healthy"~10) OR ("milkshake health"~10) OR ("milkshake nutrition"~10) OR ("milkshake energy"~10) OR ("milkshake protein"~10) OR ("yogurt health"~10) OR ("yogurt immunity"~10) OR ("yogurt weight loss"~10) OR ("yogurt protein"~10) OR ("yogurt calcium"~10) OR ("yogurt probiotics"~10) OR ("yogurt gut health"~10) OR ("yogurt healthy"~10) OR ("yoghurt health"~10) OR ("yoghurt nutritious"~10) OR ("yoghurt calcium"~10) OR ("yoghurt probiotics"~10) OR ("yoghurt healthy"~10) OR ("cream health"~10) OR ("cream nutrients"~10) OR ("cream vitamins"~10) OR ("cream energy"~10) OR ("kefir health"~10) OR ("kefir probiotics"~10) OR ("kefir metabolism"~10) OR ("kefir blood sugar"~10) OR ("kefir gut health"~10) OR ("cheese health"~10) OR ("cheese nutrition"~10) OR ("cheese nutrients"~10) OR ("cheese calcium"~10) OR ("cheese protein"~10) OR ("cheese brain development"~10) OR ("cheese vitamin B12"~10) OR ("cheese zinc"~10) OR ("cheese healthy"~10) OR ("cheeses health"~10) OR ("cheeses nutritious"~10) OR ("cheeses protein"~10) OR ("cheeses calcium"~10) OR ("butter health"~10) OR ("butter energy"~10) OR ("butter vitamin A"~10) OR ("butter metabolism"~10) OR ("buttermilk health"~10) OR ("buttermilk probiotics"~10) OR ("buttermilk nutrients"~10) OR ("buttermilk calcium"~10) OR ("whey health"~10) OR ("whey protein"~10) OR ("whey metabolism"~10) OR ("whey nutrition"~10) OR ("casein health"~10) OR ("casein protein"~10) OR ("casein digestion"~10) OR ("casein muscle"~10) OR ("lactose health"~10) OR ("lactose intolerance"~10) OR ("lactose metabolism"~10)'
            ),
            Countries=None,
            Resume_Offset='50000',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Sustainability',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((dairy OR cow OR milk OR cheese OR butter OR cream OR yogurt OR whey OR casein OR lactose OR bovaer) AND (sustainability OR ecofriendly OR "greenhouse gas" OR "climate change" OR methane OR "environmental impact" OR "net zero" OR "carbon emissions" OR conservation OR greenwash OR "renewable energy" OR biodiversity OR "carbon neutral" OR pollutant))',
            Countries=None,
            Resume_Offset='AoJwoOWem5sDPxVDQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1DQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1wdDEz',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='altmetric',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries='["uk", "fr", "de"]',
            Resume_Offset='2025,12,17',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='MAHA-Dairy News & Social Analysis',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='("Make America Healthy Again" OR MAHA OR RFK OR "Robert Kennedy" OR "Robert F. Kennedy") AND (dairy OR cow OR cows OR calf OR calves OR milk OR milks OR milked OR milking OR milkshake* OR "milk shake" OR yogurt* OR yoghurt* OR cream OR creams OR kefir* OR cheese OR cheeses OR butter OR butters OR buttermilk* OR "butter milk" OR whey OR casein OR "kay seen" OR lactose)',
            Countries=None,
            Resume_Offset='37',
            Is_Active=True,
            Last_Publication_Date='2025-10-28'
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Activism',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyvvvi6_12dqqlwa2f0qw',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker',
            Topic_Name='Combined',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='dmi_topics',
            Countries='["us"]',
            Resume_Offset='EgWczKiOAQ',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Policy & Regulation',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='("Farm Bill" OR "H.R. 8467" OR "HR 8467" OR "Farm Food and National Security Act" OR "Agricultural Act" OR "dairy trade" OR "milk trade" OR "dairy tariff" OR "milk tariff" OR "USMCA dairy" OR "NAFTA dairy" OR "dairy policy" OR "milk policy" OR "dairy regulation" OR "agricultural policy")',
            Countries=None,
            Resume_Offset='17503',
            Is_Active=True,
            Last_Publication_Date='2025-10-28'
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Animal Welfare',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420084',
            Countries='["us"]',
            Resume_Offset='97402',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit06pta_dnux9mauizd3',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='445436',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='6702',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='445447',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='9405',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='("piroctone olamine" OR "piroctone-olamine" OR "olamine piroctone" OR "piroctonealamine" OR "piroctone olaminee" OR "pirocton olamine" OR octopirox OR #piroctoneolamine OR "Piroctone Olamine" OR "Piroctone-Olamin" OR "Olamin-Piroctone" OR "Piroctonealamin" OR "Pirocton-Olamin" OR Octopirox OR "Pyridon-Ethanolamin-Salz") NOT (India OR Indien)',
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Bird Flu/H5N1',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((dairy OR dairies OR cow OR cows OR calf OR calves OR milk OR milks OR milked OR milking OR milkshake* OR yogurt* OR yoghurt* OR cream OR creams OR kefir* OR cheese OR cheeses OR butter OR butters OR buttermilk OR "butter milk" OR whey OR casein OR lactose) AND ("bird flu" OR "bird influenza" OR "avian flu" OR "avian influenza" OR H5N1 OR "H five N one" OR "influenza A" OR HPAI OR "highly pathogenic"))',
            Countries=None,
            Resume_Offset='1012',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='("Urban Waste Water Treatment Directive" OR "Urban Wastewater Treatment Directive" OR UWWTD OR "Urban Waste Water Directive" OR "Urban Wastewater Directive" OR "Directive sur le traitement des eaux urbaines résiduaires" OR "Directive sur les eaux urbaines résiduaires" OR  "Richtlinie zur Behandlung von städtischem Abwasser" OR "Richtlinie über städtisches Abwasser" OR "ERV" OR "Erweiterte Herstellerverantwortung" OR "Verursacherprinzip" OR "Quaternäre Behandlung")',
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit063sp_dnux9mauiruq',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='polimonitor',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='7',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Food Quality & Safety',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420113',
            Countries='["us"]',
            Resume_Offset='133706',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='(salicylate* OR salicylic* OR "2-hydroxybenzoic acid" OR "homosalate" OR salicylate* OR salicylique* OR "acide 2-hydroxybenzoïque" OR Salicylat* OR Salicyl* OR "2-Hydroxybenzoesäure") NOT ("ultra processed" OR "ultra-processed" OR ultraprocessed OR UPF OR "Make America Healthy Again" OR MAHA OR #MAHA OR #MakeAmericaHealthyAgain OR "RFK Jr" OR "Robert F Kennedy" OR #RFKJr OR "seed oil" OR #seedoils OR "processed food industry" OR "Big Food" OR "food industrial complex" OR #ultraprocessed OR #UPF OR #bigfood)',
            Countries=None,
            Resume_Offset='0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Policy & Regulation',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='((dairy OR milk OR cheese OR butter OR cream OR yogurt) AND ("American Dairy Association" OR "National Milk Producers Federation" OR "California Milk Advisory Board" OR "Dairy Farmers of Washington" OR "Dairy Farmers of Wisconsin" OR "Dairy Management West" OR "Dairy West" OR "Florida Dairy Farmers") AND (CMAB OR NDC OR NMPF OR bovaer OR labor OR worker OR migrant OR immigrant OR undocumented OR H-2A OR Fairlife OR "animal cruelty" OR "animal neglect" OR "legal action" OR "H.R.1603"))',
            Countries=None,
            Resume_Offset='AoJw4PPJm5gDPwRDSFJHLTExOWhocmc2MTgyOS1DSFJHLTExOWhocmc2MTgyOQ==',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Health & Wellness',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '((dairy OR milk OR milks OR milkshake* OR yogurt* OR yoghurt* OR cream OR creams OR kefir* OR cheese OR cheeses OR butter OR butters OR buttermilk* OR whey OR casein OR lactose) '
                'NEAR/10 '
                '(health OR immunity OR (weight NEAR/1 loss) OR (weight NEAR/1 management) OR nutrition OR nutritious OR nutrients OR calcium OR (vitamin NEAR/1 D) OR vitamins OR minerals OR iodine OR (vitamin NEAR/1 b12) OR zinc OR selenium OR niacin OR riboflavin OR probiotics OR (vitamin NEAR/1 A) OR potassium OR (brain NEAR/1 development) OR energy OR metabolism OR (blood NEAR/1 pressure) OR diabetes OR glucose OR healthy))'
            ),
            Countries=None,
            Resume_Offset='AoJwoOWem5sDPxVDQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1DQ0FMLTExOWhjYWwtMjAyNS0xMi0xOS1wdDEz',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Sustainability',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywhoi4_12dqqlwa2e6ga',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Policy & Regulation',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcywg6eo_12dqqlwa2f5x6',
            Countries='["us"]',
            Resume_Offset='1766102400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_influencers',
            Topic_Name='Activism',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='mcyvvvi6_12dqqlwa2f0qw',
            Countries='["us"]',
            Resume_Offset='1758240000000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Sustainability',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420083',
            Countries='["us"]',
            Resume_Offset='94909',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='altmetric',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries='["uk", "fr", "de"]',
            Resume_Offset='0,0,0',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit083lu_dnux9maujem1',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='polimonitor',
            Topic_Name='Piroctone Olamine',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries=None,
            Resume_Offset='7',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='usgovinfo',
            Topic_Name='Food Quality & Safety',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='(dairy OR dairies OR cow OR cows OR calf OR calves OR milk OR milks OR milked OR milking OR yogurt OR yoghurts OR cream OR cheese OR cheeses OR butter OR butters OR buttermilk OR buttermilks OR whey OR casein OR lactose OR kefir OR kefirs)  AND  ("food safety" OR "food quality" OR contamination OR adulteration OR pathogen* OR salmonella OR listeria OR E.coli OR spoilage OR "hazard analysis" OR HACCP OR  "product recall")',
            Countries=None,
            Resume_Offset='AoJwoInShpsDOEZSLTIwMjUtMTItMTEtMjAyNS0yMjUxMw==',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Retinol and derivates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='445443',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='22403',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Animal Welfare',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("cow separation"~20) OR ("cows separation"~20) OR ("calf separation"~20) OR ("calves separation"~20) OR ("cow colostrum deprivation"~20) OR ("cows colostrum deprivation"~20) OR ("calf colostrum deprivation"~20) OR ("calves colostrum deprivation"~20) OR ("cow welfare"~20) OR ("cows welfare"~20) OR ("calf welfare"~20) OR ("calves welfare"~20) OR ("cow factory farming"~20) OR ("cows factory farming"~20) OR ("calf factory farming"~20) OR ("calves factory farming"~20) OR ("cow cruelty"~20) OR ("cows cruelty"~20) OR ("calf cruelty"~20) OR ("calves cruelty"~20) OR ("cow abuse"~20) OR ("cows abuse"~20) OR ("calf abuse"~20) OR ("calves abuse"~20) OR ("cow neglect"~20) OR ("cows neglect"~20) OR ("calf neglect"~20) OR ("calves neglect"~20) OR ("cow dehorn*"~20) OR ("cows dehorn*"~20) OR ("calf dehorn*"~20) OR ("calves dehorn*"~20) OR ("cow tail dock*"~20) OR ("cows tail dock*"~20) OR ("calf tail dock*"~20) OR ("calves tail dock*"~20) OR ("cow overmilk*"~20) OR ("cows overmilk*"~20) OR ("calf overmilk*"~20) OR ("calves overmilk*"~20) OR ("cow mastitis"~20) OR ("cows mastitis"~20) OR ("calf mastitis"~20) OR ("calves mastitis"~20) OR ("cow artificial insemination"~20) OR ("cows artificial insemination"~20) OR ("calf artificial insemination"~20) OR ("calves artificial insemination"~20)'
            ),
            Countries=None,
            Resume_Offset='15852',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
        Row(
            Source_Name='altmetric',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries='["uk", "fr", "de"]',
            Resume_Offset='2025,12,17',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Farming, Marketplace & Industry',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420116',
            Countries='["us"]',
            Resume_Offset='96802',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Activism',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420092',
            Countries='["us"]',
            Resume_Offset='93402',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='altmetric',
            Topic_Name='Salycilates',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query=None,
            Countries='["uk", "fr", "de"]',
            Resume_Offset='2025,12,15',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='youscan',
            Topic_Name='Dairy Alternatives & Substitutions',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query='420122',
            Countries='["us"]',
            Resume_Offset='94802',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='talkwalker_engagements',
            Topic_Name='Urban Wastewater Treatment Directive',
            Project_ID='03204d6c-4539-4631-a232-8a4169863f44',
            Search_Query='mit05bf8_dnux9mauir0p',
            Countries='["uk", "fr", "de"]',
            Resume_Offset='1645574400000',
            Is_Active=True,
            Last_Publication_Date=None
        ),
        Row(
            Source_Name='dimensions',
            Topic_Name='Dairy Alternatives & Substitutions',
            Project_ID='0e5d30af-1411-4503-abf3-527620f50570',
            Search_Query=(
                '("non-dairy milk"~2) OR ("non-dairy cream"~2) OR ("non-dairy butter"~2) OR ("non-dairy yogurt"~2) OR ("non-dairy yoghurt"~2) OR ("non-dairy kefir"~2) OR ("non-dairy cheese"~2) OR ("non-dairy custard"~2) OR ("nondairy milk"~2) OR ("nondairy cream"~2) OR ("nondairy butter"~2) OR ("nondairy yogurt"~2) OR ("nondairy yoghurt"~2) OR ("nondairy kefir"~2) OR ("nondairy cheese"~2) OR ("nondairy custard"~2) OR ("dairy-free milk"~2) OR ("dairy-free cream"~2) OR ("dairy-free butter"~2) OR ("dairy-free yogurt"~2) OR ("dairy-free yoghurt"~2) OR ("dairy-free kefir"~2) OR ("dairy-free cheese"~2) OR ("dairy-free custard"~2) OR ("dairy free milk"~2) OR ("dairy free cream"~2) OR ("dairy free butter"~2) OR ("dairy free yogurt"~2) OR ("dairy free yoghurt"~2) OR ("dairy free kefir"~2) OR ("dairy free cheese"~2) OR ("dairy free custard"~2) OR ("plant-based milk"~2) OR ("plant-based cream"~2) OR ("plant-based butter"~2) OR ("plant-based yogurt"~2) OR ("plant-based yoghurt"~2) OR ("plant-based kefir"~2) OR ("plant-based cheese"~2) OR ("plant-based custard"~2) OR ("vegan milk"~2) OR ("vegan cream"~2) OR ("vegan butter"~2) OR ("vegan yogurt"~2) OR ("vegan yoghurt"~2) OR ("vegan kefir"~2) OR ("vegan cheese"~2) OR ("vegan custard"~2) OR ("alternative milk"~2) OR ("alternative cream"~2) OR ("alternative butter"~2) OR ("alternative yogurt"~2) OR ("alternative yoghurt"~2) OR ("alternative kefir"~2) OR ("alternative cheese"~2) OR ("alternative custard"~2) OR ("substitute milk"~2) OR ("substitute cream"~2) OR ("substitute butter"~2) OR ("substitute yogurt"~2) OR ("substitute yoghurt"~2) OR ("substitute kefir"~2) OR ("substitute cheese"~2) OR ("substitute custard"~2) OR ("oat milk") OR ("almond milk") OR ("cashew milk") OR ("pea milk") OR ("macadamia milk") OR ("soy milk") OR ("rice milk")'
            ),
            Countries=None,
            Resume_Offset='8764',
            Is_Active=True,
            Last_Publication_Date='2025-11-01'
        ),
    ],
    schema=bronze_metadata_topics_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_topics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save and Overwrite

# CELL ********************

# Save Sources
# -- Synapse - external Delta anchored to a path
# (df_sources.write
#     .mode("overwrite")
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .option("path", f"{meta_Path}/bronze_sources")
#     .saveAsTable("meta.bronze_sources")
# )
# -- Fabric - managed Delta table (no path)
(df_sources.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{META}.bronze_sources")
)

# Save Topics
# -- Synapse - topics save was COMMENTED OUT (why meta.bronze_topics was empty)
# (df_topics.write
#     .mode("overwrite")
#     .format("delta")
#     .option("overwriteSchema", "true")
#     .option("path", f"{meta_Path}/bronze_topics")
#     .saveAsTable("meta.bronze_topics")
# )
# -- Fabric - managed Delta table (no path) - ENABLED so topics actually seed
(df_topics.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{META}.bronze_topics")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
