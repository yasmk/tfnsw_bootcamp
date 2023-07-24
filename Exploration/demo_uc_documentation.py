# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Adding comments and constraints to your data
# MAGIC
# MAGIC In the following example, we are creating a json file with **[comments](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-comment.html)** for discoverability and business documentation purposes. We are also adding **[constraints](https://learn.microsoft.com/en-gb/azure/databricks/tables/constraints)** (public preview) to tables, for quality and reference purposes, e.g. primary and foreign keys and quality constraints 
# MAGIC
# MAGIC **NOTE:** If you set a primary key ensure column is set to NOT NULL i.e.:
# MAGIC `ALTER TABLE table_name ALTER COLUMN column SET NOT NULL;`

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to import a custom json config file for our catalog, you can then reuse this code for each catalog a team creates. See file `config.json`

# COMMAND ----------

# creating the file
catalog_documentation = [
    {
        "catalog_name": "lisa_sherin_dac_demo_catalog",
        "catalog_comment": "Catalog for demos and sandboxing for NSW Public Sector. Sources include TfNSW [OpenData](https://opendata.transport.nsw.gov.au/)",
        "schema_documentation": [
            {
                "schema_name": "ref_timetables",
                "schema_comment": "TfNSW has been supplying a GTFS dataset for journey planning for many years. Static timetables, stop locations, and route shape information in General Transit Feed Specification (GTFS) format for all operators, including regional, trackwork and transport routes not available in realtime feeds. Sourced from [OpenData](https://opendata.transport.nsw.gov.au/dataset/timetables-complete-gtfs) for more information.",
"table_documentation": [
    {
        "table_name": "pathways",
        "table_comment": "This table provides information for the customer about how to book the service",
        "column_comments": {
            "pathway_id": "An ID that uniquely identifies the pathway",
            "from_stop_id": "Location at which the pathway begins. It contains a stop_id that identifies a platform, entrance/exit, generic node or boarding area from the stops.txt file",
            "to_stop_id": "Location at which the pathway ends. It contains a stop_id that identifies a platform, entrance/exit, generic node or boarding area from the stops.txt file",
            "pathway_mode": "Type of pathway between the specified (from_stop_id, to_stop_id) pair.",
            "is_bidirectional": "Indicates in which direction the pathway can be used",
            "traversal_time": "Last time on the last day before travel to make the booking request.",
        },
        "table_constraints": ["pathway_pk PRIMARY KEY(pathway_id)"]
    },
    {
        "table_name": "levels",
        "table_comment": "This table provides information about the locations where pick up or drop off is possible",
        "column_comments": {
            "level_id": "Id of the level that can be referenced from stops.txt.",
            "level_index": "Collection of 'Feature' objects describing the locations",
            "level_name": "Optional name of the level (that matches level lettering/numbering used inside the building or the station)",
        },
         "table_constraints": ["levels_pk PRIMARY KEY(level_id)"],
    },
]

            },
        ],
    },
]

# COMMAND ----------

class Table:
    def __init__(self, table_name, table_comment, column_comments, table_constraints):
        self.table_name = table_name
        self.table_comment = table_comment
        self.column_comments = column_comments
        self.table_constraints = table_constraints

    def set_comments_and_constraints(self):
        # Set table comment
        spark.sql(f'COMMENT ON TABLE {self.table_name} IS "{self.table_comment}";')

        # Set column comments
        for column, comment in self.column_comments.items():
            spark.sql(f'ALTER TABLE {self.table_name} ALTER COLUMN {column} COMMENT "{comment}"')

        # Set table constraints
        for constraint in self.table_constraints:
            spark.sql(f'ALTER TABLE {self.table_name} ADD CONSTRAINT {constraint}')

class Schema:
    def __init__(self, schema_name, schema_comment):
        self.schema_name = schema_name
        self.schema_comment = schema_comment
        self.tables = []

    def add_table(self, table):
        self.tables.append(table)

    def set_comments_and_constraints(self):
        # Set schema comment
        spark.sql(f'USE SCHEMA {self.schema_name}')
        spark.sql(f'COMMENT ON SCHEMA {self.schema_name} IS "{self.schema_comment}";')

        # Set comments and constraints for each table
        for table in self.tables:
            table.set_comments_and_constraints()

class Catalog:
    def __init__(self, catalog_name, catalog_comment):
        self.catalog_name = catalog_name
        self.catalog_comment = catalog_comment
        self.schemas = []

    def add_schema(self, schema):
        self.schemas.append(schema)

    def set_comments_and_constraints(self):
        # Set catalog comment
        spark.sql(f'USE CATALOG {self.catalog_name}')
        spark.sql(f'COMMENT ON CATALOG {self.catalog_name} IS "{self.catalog_comment}";')

        # Set comments and constraints for each schema
        for schema in self.schemas:
            schema.set_comments_and_constraints()

# COMMAND ----------

import json

# Read the JSON config file
def read_config_file(file_path):
    with open(file_path, 'r') as file:
        config_data = json.load(file)
    return config_data

# Create instances of Table, Schema, and Catalog classes
def create_instances_from_config(config_data):
    catalogs = config_data.get("catalogs", [])
    all_catalogs = []

    for catalog_data in catalogs:
        catalog_name = catalog_data.get("catalog_name")
        catalog_comment = catalog_data.get("catalog_comment")
        catalog = Catalog(catalog_name, catalog_comment)

        schemas = catalog_data.get("schemas", [])
        for schema_data in schemas:
            schema_name = schema_data.get("schema_name")
            schema_comment = schema_data.get("schema_comment")
            schema = Schema(schema_name, schema_comment)

            tables = schema_data.get("tables", [])
            for table_data in tables:
                table_name = table_data.get("table_name")
                table_comment = table_data.get("table_comment")
                column_comments = table_data.get("column_comments", {})
                table_constraints = table_data.get("table_constraints", [])
                table = Table(table_name, table_comment, column_comments, table_constraints)
                schema.add_table(table)

            catalog.add_schema(schema)

        all_catalogs.append(catalog)

    return all_catalogs

# Function to apply the documentation and constraints using the Catalog class
def add_documentation_and_constraints_from_config(config_data):
    catalogs = create_instances_from_config(config_data)
    for catalog in catalogs:
        catalog.set_comments_and_constraints()


# COMMAND ----------

# Example usage
config_file_path = "/Workspace/Repos/odl_instructor_580356@databrickslabs.com/tfnsw_bootcamp/Exploration/documentation_config.json"
config_data = read_config_file(config_file_path)
add_documentation_and_constraints_from_config(config_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now let's check the table, you should be able to find the table level comment and any constraints created
# MAGIC DESCRIBE TABLE EXTENDED pathways; 
