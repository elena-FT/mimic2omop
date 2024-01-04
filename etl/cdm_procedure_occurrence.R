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
dbExecute(con, "DROP TABLE IF EXISTS cdm_procedure_occurrence;")

procedure_occurrence <- dbSendQuery(con, "SELECT * FROM demo_cdm.procedure_occurrence;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
procedureevents_file <- file.path(mimic_folder, "icu", "procedureevents.csv.gz")
df_mimic_procedureevents <- fread(procedureevents_file)

d_items_file <- file.path(mimic_folder, "icu", "d_items.csv.gz")
df_mimic_d_items <- fread(d_items_file)

hcpcs_file <- file.path(mimic_folder, "hosp", "d_hcpcs.csv.gz")
df_mimic_hcps <- fread(hcpcs_file)

col_procedure_occurrence_info <- dbColumnInfo(procedure_occurrence)
col_cdm_procedure_occurrence_info <- paste0(
  paste0(col_procedure_occurrence_info$name, " ", col_procedure_occurrence_info$type, " ", ifelse(col_procedure_occurrence_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_procedure_occurrence (\n",
  col_cdm_procedure_occurrence_info, "\n);"
)

dbClearResult(procedure_occurrence)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

# Mapping
concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept, -1)
dbClearResult(concept)

resconcept_filtered <- resconcept %>%
  filter(domain_id == 'Procedure')

df_mimic_d_items_filtered <- df_mimic_d_items %>%
  filter(linksto == 'procedureevents')

mimic_info <- df_mimic_d_items_filtered[, c("itemid", "label", "abbreviation")]

# Utiliser la fonction de mapping
mimic_omop_mapping <- procedure_concept_mapping(mimic_info, resconcept_filtered)

# Affichage des résultats
result_procedure <- df_mimic_procedureevents %>%
  left_join(mapping_table, by = "subject_id") %>%
  left_join(mimic_omop_mapping, by = c("itemid" = "item_id_mimic")) %>%
  mutate(
    procedure_occurrence_id = row_number(),
    procedure_concept_id = as.integer(concept_id_cdm),
    procedure_date = as.Date(starttime), 
    procedure_datetime = as.POSIXct(storetime),
    procedure_type_concept_id = as.integer(concept_type_id_cdm),
    modifier_concept_id = as.integer(NA),
    quantity = as.integer(value),
    provider_id = as.integer(hadm_id),
    visit_occurrence_id = as.integer(hadm_id),
    visit_detail_id = as.integer(NA),
    procedure_source_value = as.character(concept_source_value),
    procedure_source_concept_id = as.integer(NA),
    modifier_source_value = as.character(NA)
  ) %>%
  select(
    procedure_occurrence_id, person_id, procedure_concept_id,
    procedure_date, procedure_datetime, procedure_type_concept_id,
    modifier_concept_id, quantity, provider_id, visit_occurrence_id,
    visit_detail_id, procedure_source_value, procedure_source_concept_id,
    modifier_source_value
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_procedure_occurrence", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_procedure_occurrence <- dbSendQuery(con, "SELECT * FROM cdm_procedure_occurrence;")
fetch(df_cdm_procedure_occurrence, n=-1)

dbDisconnect(con)
