library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger le fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")
source("etl/mapping_functions.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Supprimer la table cdm_death si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_observation;")

observation <- dbSendQuery(con, "SELECT * FROM demo_cdm.observation;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
admission_file <- file.path(mimic_folder, "hosp", "admissions.csv.gz")
df_mimic_admissions <- fread(admission_file)

d_items_file <- file.path(mimic_folder, "icu", "d_items.csv.gz")
df_mimic_d_items <- fread(d_items_file)

chartevents_file <- file.path(mimic_folder, "icu", "chartevents.csv.gz")
df_mimic_chartevents <- fread(chartevents_file)

col_observation_info <- dbColumnInfo(observation)
col_cdm_observation_info <- paste0(
  paste0(col_observation_info$name, " ", col_observation_info$type, " ", ifelse(col_observation_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_observation (\n",
  col_cdm_observation_info, "\n);"
)

dbClearResult(observation)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

# Mapping
concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept, -1)
dbClearResult(concept)

resconcept_filtered <- resconcept %>%
  filter(domain_id == 'Observation')

df_mimic_d_items_filtered <- df_mimic_d_items %>%
  filter(linksto == 'chartevents')

mimic_info <- df_mimic_d_items_filtered[, c("itemid", "label", "abbreviation")]

# Utiliser la fonction de mapping
mimic_omop_mapping <- observation_concept_mapping(mimic_info, resconcept_filtered)

# Affichage des résultats
result <- df_mimic_chartevents %>%
  left_join(mapping_table, by = "subject_id") %>%
  left_join(mimic_omop_mapping, by = c("itemid" = "item_id_mimic")) %>%
  mutate(
    observation_id = row_number(),
    observation_concept_id = as.integer(cdm_concept_id),
    observation_date = as.Date(charttime),
    observation_datetime = as.POSIXct(charttime),
    observation_type_concept_id = as.integer(NA),
    value_as_number = as.numeric(valuenum),
    value_as_string = as.character(value),
    value_as_concept_id = as.integer(NA),
    qualifier_concept_id = as.integer(NA),
    unit_concept_id = as.integer(NA), #Pas trouvé de concept des units
    provider_id = as.integer(caregiver_id), 
    visit_occurrence_id = as.integer(hadm_id),
    visit_detail_id = as.integer(NA), 
    observation_source_value = as.character(cdm_concept_code),
    observation_source_concept_id = as.integer(cdm_concept_id),
    unit_source_value = as.character(NA),
    qualifier_source_value = as.character(NA)
  ) %>%
  select(
    observation_id, person_id, observation_concept_id,
    observation_date, observation_datetime, observation_type_concept_id,
    value_as_number, value_as_string, value_as_concept_id,
    qualifier_concept_id, unit_concept_id, provider_id,
    visit_occurrence_id, visit_detail_id, observation_source_value,
    observation_source_concept_id, unit_source_value, qualifier_source_value
  )


# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_observation", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_observation <- dbSendQuery(con, "SELECT * FROM cdm_observation;")
fetch(df_cdm_observation, n=-1)

dbDisconnect(con)
