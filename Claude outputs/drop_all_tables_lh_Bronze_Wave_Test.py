# Drop all tables in the lh_Bronze_Wave_Test lakehouse
#
# Run this as a PySpark cell in a Fabric notebook with lh_Bronze_Wave_Test attached as the
# default lakehouse (or set LAKEHOUSE_NAME to the exact name Fabric shows if it isn't
# attached -- Spark resolves it as a catalog/database name either way).
#
# Iterates every schema in the lakehouse -- including "dbo", since this lakehouse has schema
# support enabled -- and drops every table Spark's catalog recognizes there. A plain %%sql
# cell can't do this (no loop construct), so the discovery + DROP TABLE calls are done here
# in Python via spark.sql().
#
# NOTE ON THE PARSE ERROR FROM THE FIRST VERSION: Fabric's workspace name ("Wave Data
# Medallion") is itself the catalog part of the fully-qualified table name, and it has a
# space in it. Gluing catalog.schema.table together as one raw SQL string and only
# backtick-quoting pieces of it broke as soon as Spark's parser hit the unquoted space in
# "Wave Data". This version sidesteps that entirely: spark.catalog.setCurrentDatabase()
# switches into each schema through the Catalog API (which handles a space-containing
# catalog/workspace name correctly on its own), so DROP TABLE only ever needs the plain
# table name -- no manual quoting of the workspace or schema name required.
#
# NOTE: this only drops tables Spark's catalog can actually see. Any leftover "Unidentified"
# folders under Tables/ (from a write that landed outside a schema, before the recent fix to
# nb_CopyTableToBronze) won't be listed here, since Fabric never registered them as tables in
# the first place -- see the optional cleanup cell at the bottom to remove those directly.

LAKEHOUSE_NAME = "lh_Bronze_Wave_Test"

databases = [db.name for db in spark.catalog.listDatabases() if db.name != "information_schema"]

dropped = []
for db in databases:
    spark.catalog.setCurrentDatabase(db)
    for t in spark.catalog.listTables(db):
        spark.sql(f"DROP TABLE IF EXISTS `{t.name}`")
        dropped.append(f"{db}.{t.name}")

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
