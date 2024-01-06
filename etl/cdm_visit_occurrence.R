library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger les fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")
source("etl/usefull_fonctions.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

visit_occurrence <- dbSendQuery(con, "SELECT * FROM demo_cdm.visit_occurrence;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
admission_file <- file.path(mimic_folder, "hosp", "admissions.csv.gz")
df_mimic_admission <- fread(admission_file)

col_vo_info <- dbColumnInfo(visit_occurrence)
col_cdm_vo_info <- paste0(
  paste0(col_vo_info$name, " ", col_vo_info$type, " ", ifelse(col_vo_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

dbClearResult(visit_occurrence)

# Supprimer la table cdm_visit_occurrence si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_visit_occurrence;")

# A executer qu'une fois (pour creer la table)
dbExecute(con, paste0(
  "CREATE TABLE cdm_visit_occurrence (\n",
  col_cdm_vo_info, "\n);"))

result <- df_mimic_admission %>% 
  left_join(mapping_table, by = "subject_id") %>%
  mutate(
    visit_occurrence_id = hadm_id,
    visit_concept_id = as.integer(NA),
    visit_start_date = admittime,
    visit_start_datetime = as.integer(NA),
    visit_end_date = dischtime,
    visit_end_datetime = as.integer(NA),
    visit_type_concept_id = as.integer(NA), # admission_type (don't get the right id)
    provider_id = as.integer(NA), # admit_provider_id (don't get the right mapping),
    care_site_id = as.integer(NA), # admission_location (don't get the right id)
    visit_source_value = as.integer(NA),
    visit_source_concept_id = as.integer(NA),
    admitting_source_concept_id = as.integer(NA),
    admitting_source_value = as.integer(NA),
    discharge_to_concept_id = as.integer(NA),
    discharge_to_source_value = as.integer(NA),
    preceding_visit_occurrence_id = as.integer(NA)
  ) %>%
  select(
    person_id, 
    visit_occurrence_id, 
    visit_concept_id, 
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitting_source_concept_id,
    admitting_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_visit_occurrence
dbWriteTable(con, "cdm_visit_occurrence", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_person
getDataFromTable(con, "cdm_visit_occurrence", -1)

dbDisconnect(con)
