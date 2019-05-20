# py_dbmigration
Utilities used to move data around different data sources


-included Redaction file parameter that will redact columns based on content of a redaciton configuration file.
-metasource files now will need to have a crc column added as all files inventoried will also get a checksum associated.
-duplicate checksum across different files will not get imported and will yeild a "Failed" import state for subseqent file
- upsert_function_name will be a config item to identify the stored procedure that will do the upsert.
skipifexists parameter added so no unzip a file if the directory already exists.  This will speed up testing as we would not want to re-extract files that are already extracted