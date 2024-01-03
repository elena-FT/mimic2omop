library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger le fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Supprimer la table cdm_death si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_care_site;")

care_site <- dbSendQuery(con, "SELECT * FROM demo_cdm.care_site;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
transfers_file <- file.path(mimic_folder, "hosp", "transfers.csv.gz")
df_mimic_transfers <- fread(transfers_file)

col_care_site_info <- dbColumnInfo(care_site)
col_cdm_care_site_info <- paste0(
  paste0(col_care_site_info$name, " ", col_care_site_info$type, " ", ifelse(col_care_site_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_care_site (\n",
  col_cdm_care_site_info, "\n);"
)

dbClearResult(care_site)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

# Affichage des résultats
result <- df_mimic_transfers %>%
  mutate(
    care_site_id = as.integer(NA),
    care_site_name = careunit,
    place_of_service_concept_id = as.integer(NA),
    location_id = as.integer(NA),
    care_site_source_value = careunit,
    place_of_service_source_value = careunit
  ) %>%
  select(
    care_site_id, care_site_name, place_of_service_concept_id, 
    location_id, care_site_source_value, place_of_service_source_value
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_care_site", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_care_site <- dbSendQuery(con, "SELECT * FROM cdm_care_site;")
fetch(df_cdm_care_site, n=-1)

dbDisconnect(con)
