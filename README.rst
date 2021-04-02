==============
py_dbmigration
==============


Project to move data driven by yaml file


Description
===========

currently supports POSTGRES
TODO:
mssql, mysql, msaccess, sqlite, s3,excel, parquet

Make Bundled Executeable:
make exe


tools:
	data_load = toold to load data based on yaml config file
	extract_schema_to_sqitch = tool to extract db tables to individual script files
	meta_source = tool to look at data load process running
	sql_runner = tool to run sql base on a yaml file for each day

Note
====

This project has been set up using PyScaffold 3.1. For details and usage
information on PyScaffold see https://pyscaffold.org/.
