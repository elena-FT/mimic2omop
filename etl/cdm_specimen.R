library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger le fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")
source("etl/usefull_fonctions.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Supprimer la table cdm_death si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_specimen;")

specimen <- dbSendQuery(con, "SELECT * FROM demo_cdm.specimen;")
#vocab <- dbSendQuery(con, "SELECT * FROM demo_cdm.vocabulary;")
concept_recommended <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept_recommended;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
mbe_file <- file.path(mimic_folder, "hosp", "microbiologyevents.csv.gz")
df_mimic_mbe <- fread(mbe_file)

col_specimen_info <- dbColumnInfo(specimen)
col_cdm_specimen_info <- paste0(
  paste0(col_specimen_info$name, " ", col_specimen_info$type, " ", ifelse(col_specimen_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_specimen (\n",
  col_cdm_specimen_info, "\n);"
)

dbClearResult(specimen)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

#Mapping

concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept, -1)
dbClearResult(concept)

mimic_info <- df_mimic_mbe[, c("micro_specimen_id", "spec_type_desc")]

# Utiliser la fonction de mapping
mimic_omop_mapping <- specimen_concept_mapping(mimic_info, resconcept)

result <- df_mimic_mbe %>%
  left_join(mapping_table, by = "subject_id") %>%
  left_join(mimic_omop_mapping, by = c("micro_specimen_id" = "micro_specimen_id")) %>%
  mutate(
    specimen_id = row_number(),
    specimen_concept_id = as.integer(specimen_concept_id),
    specimen_type_concept_id = as.integer(specimen_type_concept_id),
    specimen_date = as.Date(charttime),
    specimen_datetime = as.POSIXct(charttime),
    quantity = quantity,
    unit_concept_id = as.integer(NA),
    anatomic_site_concept_id = as.integer(NA),
    disease_status_concept_id = as.integer(NA),
    specimen_source_id = as.character(NA),
    specimen_source_value = as.character(NA),
    unit_source_value = as.character(NA),
    anatomic_site_source_value = as.character(NA),
    disease_status_source_value = as.character(NA)
  ) %>%
  select(
    person_id, specimen_id, specimen_concept_id, specimen_type_concept_id,
    specimen_date, specimen_datetime, quantity, unit_concept_id,
    anatomic_site_concept_id, disease_status_concept_id,
    specimen_source_id, specimen_source_value, unit_source_value,
    anatomic_site_source_value, disease_status_source_value
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_specimen", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_specimen <- dbSendQuery(con, "SELECT * FROM cdm_specimen;")
fetch(df_cdm_specimen, n=-1)

dbDisconnect(con)
