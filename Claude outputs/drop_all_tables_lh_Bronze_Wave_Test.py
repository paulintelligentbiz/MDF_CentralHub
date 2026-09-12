# Drop all tables in the lh_Bronze_Wave_Test lakehouse
#
# Run this as a PySpark cell in a Fabric notebook with lh_Bronze_Wave_Test attached as the
# default lakehouse.
#
# WHY THE EARLIER VERSIONS FAILED: spark.catalog.listDatabases() in this workspace returns
# db.name as a single compound string, "Wave Data Medallion.lh_Bronze_Wave_Test" -- the
# workspace name IS the catalog part of the fully-qualified name, and it has a space in it.
# The first version built that into raw SQL text without quoting it. The second version
# tried to sidestep that by handing the same raw string to spark.catalog.setCurrentDatabase()
# / listTables() instead -- but those Catalog API calls re-parse the string internally too,
# so the identical "Syntax error at or near 'Data'" came right back from inside the API, not
# from anything this script wrote to spark.sql() directly.
#
# This version never lets Spark see an unquoted compound name: it splits each returned db
# name on its first "." and backtick-quotes the two pieces itself before building any SQL,
# using SHOW TABLES IN / DROP TABLE with that pre-quoted qualifier throughout -- so a
# space (or any other special character) inside the workspace name can't break parsing.

LAKEHOUSE_NAME = "lh_Bronze_Wave_Test"


def quote_ident(name: str) -> str:
    """Backtick-quote a single identifier segment, escaping any embedded backtick."""
    return f"`{name.replace('`', '``')}`"


def quote_qualified(db_name: str) -> str:
    """db_name comes back as '<workspace name>.<lakehouse name>' in this workspace (one
    string). Split on the first '.' and quote each part separately -- never re-parse the
    combined string as SQL."""
    parts = db_name.split(".", 1)
    return ".".join(quote_ident(p) for p in parts)


databases = [db.name for db in spark.catalog.listDatabases() if db.name != "information_schema"]

dropped = []
for db_name in databases:
    qualified_db = quote_qualified(db_name)
    try:
        tables = spark.sql(f"SHOW TABLES IN {qualified_db}").collect()
    except Exception as e:
        print(f"Skipping {db_name} -- could not list tables: {e}")
        continue
    for row in tables:
        table_name = row["tableName"]
        full_name = f"{qualified_db}.{quote_ident(table_name)}"
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        dropped.append(f"{db_name}.{table_name}")

print(f"Dropped {len(dropped)} table(s) from {LAKEHOUSE_NAME}:")
for name in dropped:
    print(f"  - {name}")

# ---------------------------------------------------------------------------------------
# OPTIONAL: clean up orphaned Tables/<name> folders that were never registered as tables --
# e.g. anything still sitting outside the dbo schema from before the schema-path fix (DimAccount,
# DimChannel, DimCustomer, DimDate showed up this way earlier). These won't appear in
# spark.catalog and so aren't touched by the loop above.
#
# List first and eyeball the results before deleting anything -- notebookutils.fs.rm is
# permanent. Fill in WORKSPACE_ID / LAKEHOUSE_ID below (the nb_CopyTableToBronze notebook's
# defaults are 947d3136-33ac-458a-be73-ac7dc38afaa5 / 649b7795-2e22-4627-8b25-9749a6f492f0 --
# confirm these match lh_Bronze_Wave_Test before using them).
# ---------------------------------------------------------------------------------------

# import notebookutils
#
# WORKSPACE_ID = "947d3136-33ac-458a-be73-ac7dc38afaa5"
# LAKEHOUSE_ID = "649b7795-2e22-4627-8b25-9749a6f492f0"
# tables_root = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables"
#
# for item in notebookutils.fs.ls(tables_root):
#     print(item.name, "DIR" if item.isDir else "FILE", item.size)
#
# # Once you've confirmed which of these are orphans (not listed as tables above), delete them:
# # notebookutils.fs.rm(f"{tables_root}/<OrphanFolderName>", recurse=True)
