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

bronze_altmetric_schema = StructType(
    [
        StructField("error", StringType(), True),
        StructField("attempted_url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("doi", StringType(), True),
        StructField("pmid", StringType(), True),
        StructField("isbns", StringType(), True),
        StructField("altmetric_jid", StringType(), True),
        StructField("issns", ArrayType(StringType()), True),
        StructField("journal", StringType(), True),
        StructField(
            "cohorts",
            StructType(
                [
                    StructField("sci", LongType(), True),
                    StructField("pub", LongType(), True),
                    StructField("com", LongType(), True),
                    StructField("doc", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "context",
            StructType(
                [
                    StructField(
                        "all",
                        StructType(
                            [
                                StructField("count", LongType(), True),
                                StructField("mean", FloatType(), True),
                                StructField("rank", LongType(), True),
                                StructField("pct", LongType(), True),
                                StructField("higher_than", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "journal",
                        StructType(
                            [
                                StructField("count", LongType(), True),
                                StructField("mean", FloatType(), True),
                                StructField("rank", LongType(), True),
                                StructField("pct", LongType(), True),
                                StructField("higher_than", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "similar_age_3m",
                        StructType(
                            [
                                StructField("count", LongType(), True),
                                StructField("mean", FloatType(), True),
                                StructField("rank", LongType(), True),
                                StructField("pct", LongType(), True),
                                StructField("higher_than", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "similar_age_journal_3m",
                        StructType(
                            [
                                StructField("count", LongType(), True),
                                StructField("mean", FloatType(), True),
                                StructField("rank", LongType(), True),
                                StructField("pct", LongType(), True),
                                StructField("higher_than", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("authors", ArrayType(StringType()), True),
        StructField("type", StringType(), True),
        StructField("handles", StringType(), True),
        StructField("pubdate", LongType(), True),
        StructField("epubdate", LongType(), True),
        StructField("dimensions_publication_id", StringType(), True),
        StructField("altmetric_id", LongType(), True),
        StructField("schema", StringType(), True),
        StructField("is_oa", StringType(), True),
        StructField(
            "publisher_subjects",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("scheme", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("cited_by_posts_count", LongType(), True),
        StructField("cited_by_tweeters_count", LongType(), True),
        StructField("cited_by_accounts_count", LongType(), True),
        StructField("last_updated", LongType(), True),
        StructField("score", FloatType(), True),
        StructField(
            "history",
            StructType(
                [
                    StructField("1y", FloatType(), True),
                    StructField("6m", FloatType(), True),
                    StructField("3m", FloatType(), True),
                    StructField("1m", FloatType(), True),
                    StructField("1w", FloatType(), True),
                    StructField("6d", FloatType(), True),
                    StructField("5d", FloatType(), True),
                    StructField("4d", FloatType(), True),
                    StructField("3d", FloatType(), True),
                    StructField("2d", FloatType(), True),
                    StructField("1d", FloatType(), True),
                    StructField("at", FloatType(), True),
                ]
            ),
            True,
        ),
        StructField("url", StringType(), True),
        StructField("added_on", LongType(), True),
        StructField("published_on", LongType(), True),
        StructField("scopus_subjects", ArrayType(StringType()), True),
        StructField(
            "readers",
            StructType(
                [
                    StructField("citeulike", StringType(), True),
                    StructField("mendeley", StringType(), True),
                    StructField("connotea", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("readers_count", LongType(), True),
        StructField(
            "images",
            StructType(
                [
                    StructField("small", StringType(), True),
                    StructField("medium", StringType(), True),
                    StructField("large", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("details_url", StringType(), True),
        StructField("pmc", StringType(), True),
        StructField("cited_by_msm_count", LongType(), True),
        StructField("abstract", StringType(), True),
        StructField("abstract_source", StringType(), True),
        StructField("subjects", ArrayType(StringType()), True),
        StructField("cited_by_wikipedia_count", LongType(), True),
        StructField("cited_by_patents_count", LongType(), True),
        StructField("cited_by_policies_count", LongType(), True),
        StructField("ads_id", StringType(), True),
        StructField("cited_by_feeds_count", LongType(), True),
        StructField("uri", StringType(), True),
        StructField("cited_by_fbwalls_count", LongType(), True),
        StructField("cited_by_videos_count", LongType(), True),
        StructField("handle", StringType(), True),
        StructField("cited_by_gplus_count", LongType(), True),
        StructField("cited_by_rdts_count", LongType(), True),
        StructField("cited_by_peer_review_sites_count", LongType(), True),
        StructField("arxiv_id", StringType(), True),
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
