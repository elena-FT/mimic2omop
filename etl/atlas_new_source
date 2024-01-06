library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger les fichier de connexion
source("etl/connect_broadsea.R")
source("etl/usefull_fonctions.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

tables_list <- dbListTables(con)
tables_list[grep("daimon", tables_list)]

getTablesInSchema(con, "demo_cdm_results")

source <- getDataFromTable(con, "webapi.source")
source_daimon <- getDataFromTable(con, "webapi.source_daimon")

getDataFromTable(con, webapi.achilles_results)

dbExecute(con, "DELETE FROM webapi.source WHERE source_id = 2 AND EXISTS (SELECT 1 FROM webapi.source WHERE source_id = 2);")

dbExecute(con, "INSERT INTO webapi.source (source_id, source_name, source_key, source_connection, source_dialect, is_cache_enabled)
VALUES (2, 'OHDSI Database Test', 'TEST', 'jdbc:postgresql://broadsea-atlasdb:5432/postgres?user=postgres&password=mypass', 'postgresql', TRUE);")

dbExecute(con, "DELETE FROM webapi.source_daimon WHERE source_id = 2 AND EXISTS (SELECT 1 FROM webapi.source_daimon WHERE source_id = 2);")

# dbExecute(con, "INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
# VALUES (4, 2, 1, 'demo_cdm', 10);")

dbExecute(con, "INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority) 
SELECT nextval('webapi.source_daimon_sequence'), source_id, 0, 'demo_cdm', 0
FROM webapi.source
WHERE source_key = 'TEST'
;")

dbExecute(con, "INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority) 
SELECT nextval('webapi.source_daimon_sequence'), source_id, 1, 'demo_cdm', 1
FROM webapi.source
WHERE source_key = 'TEST'
;")

dbExecute(con, "INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority) 
SELECT nextval('webapi.source_daimon_sequence'), source_id, 2, 'demo_cdm_results', 1
FROM webapi.source
WHERE source_key = 'TEST'
;")

dbExecute(con, "DELETE FROM webapi.source_daimon WHERE source_id = 2 AND EXISTS (SELECT 1 FROM webapi.source_daimon WHERE source_id = 2);")

dbDisconnect(con)
