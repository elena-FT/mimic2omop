library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger les fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Afficher les tables dans la base de données
dbListTables(con)
condition_occurrence <- dbSendQuery(con, "SELECT * FROM demo_cdm.condition_occurrence;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
diagnoses_file <- file.path(mimic_folder, "hosp", "diagnoses_icd.csv.gz")
df_mimic_diagnoses <- fread(diagnoses_file)
diagnoses_file <- file.path(mimic_folder, "hosp", "d_icd_diagnoses.csv.gz")
df_mimic_diagnoses_d <- fread(diagnoses_file)

col_co_info <- dbColumnInfo(condition_occurrence)
col_cdm_co_info <- paste0(
  paste0(col_co_info$name, " ", col_co_info$type, " ", ifelse(col_co_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_condition_occurrence (\n",
  col_cdm_co_info, "\n);"
)

dbClearResult(condition_occurrence)

# Supprimer la table cdm_condition_occurrence si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_condition_occurrence;")

# A executer qu'une fois (pour creer la table)
dbExecute(con, query_create_table)

# Récupère les tables de concepts
concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept , -1)
dbClearResult(concept)

concept_relationship <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept_relationship;")
res_concept_relationship <- dbFetch(concept_relationship , -1)
dbClearResult(concept_relationship)

# Crée la table de mapping pour les concept_id
mapping_table_condition <- df_mimic_diagnoses_d %>%
  select(icd_code, long_title) %>%
  inner_join(resconcept, by = c("long_title" = "concept_name")) %>%
  mutate(concept_name = long_title) %>%
  filter(domain_id == "Condition") %>%
  select(
    concept_name,
    concept_id,
    icd_code
  )

df_mimic_diagnoses_mapped <- df_mimic_diagnoses %>%
  left_join(mapping_table_condition, by = ("icd_code"))

# Crée les données finales
result <- df_mimic_diagnoses_mapped %>% 
  left_join(mapping_table, by = "subject_id") %>%
  mutate(
    condition_occurrence_id = row_number(),
    condition_concept_id = ifelse(concept_id == 'NA', 0, as.integer(concept_id)),
    condition_start_date = as.Date(NA),
    condition_start_datetime = as.POSIXct(NA),
    condition_end_date = as.Date(NA),
    condition_end_datetime = as.POSIXct(NA),
    condition_type_concept_id = as.integer(NA),
    stop_reason = as.character(NA),
    provider_id = as.integer(NA),
    visit_occurrence_id = hadm_id,
    visit_detail_id = as.integer(NA),
    condition_source_value = as.integer(NA),
    condition_source_concept_id = as.integer(NA),
    condition_status_source_value = as.character(NA),
    condition_status_concept_id = as.character(NA)
  ) %>%
  select(
    person_id,
    condition_occurrence_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value,
    condition_status_concept_id
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_condition_occurrence
dbWriteTable(con, "cdm_condition_occurrence", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_condition_occurrence
df_cdm_condition_occurrence <- dbSendQuery(con, "SELECT * FROM cdm_condition_occurrence;")
fetch(df_cdm_condition_occurrence, n=-1)

dbDisconnect(con)
