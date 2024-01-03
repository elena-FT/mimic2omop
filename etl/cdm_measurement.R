# Charger les bibliothèques
library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger les fichiers de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Supprimer la table cdm_death si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_measurement;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
labevents_file <- file.path(mimic_folder, "hosp", "labevents.csv.gz")
df_mimic_labevents <- fread(labevents_file)

d_labitems_file <- file.path(mimic_folder, "hosp", "d_labitems.csv.gz")
df_mimic_d_labitems <- fread(d_labitems_file)

# Créer la table cdm_measurement
measurement <- dbSendQuery(con, "SELECT * FROM demo_cdm.measurement;")
col_measurement_info <- dbColumnInfo(measurement)
col_cdm_measurement_info <- paste0(
  paste0(col_measurement_info$name, " ", col_measurement_info$type, " ", ifelse(col_measurement_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)
query_create_table <- paste0(
  "CREATE TABLE cdm_measurement (\n",
  col_cdm_measurement_info, "\n);"
)
dbClearResult(measurement)
dbExecute(con, query_create_table)

# Mapping pour concept_name
concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept , -1)
dbClearResult(concept)

resconcept_filtered <- resconcept %>%
  filter(domain_id == 'Measurement')

mimic_info <- df_mimic_d_labitems[, c("itemid", "label")]

# Initialisez un dictionnaire vide
mimic_omop_mapping <- data.frame(
  item_id_mimic = mimic_info$itemid,
  label_mimic = mimic_info$label,
  concept_name_omop = NA,
  concept_code_omop = NA,
  concept_id_omop = NA
)

# Correspondances concept_name et concept_code
concept_mapping <- list(
  "Hemoglobin" = c("718-7"),
  "Glucose" = c("2345-7"),
  "Calcium" = c("17861-6"),
  "Temperature" = c("8331-1"),
  "Alanine" = c("1742-6"),
  "Platelet" = c("26515-7"),
  "Thyroxine" = c("3024-7"),
  "Hematocrit" = c("20570-8"),
  "White Blood" = c("26464-8"),
  "Urea nitrogen" = c("3094-0"),
  "Aspartate aminotransferase" = c("1920-8"),
  "Chloride" = c("2075-0"),
  "Creatinine" = c("2075-0"),
  "Sodium" = c("2951-2"),
  "Protein" = c("2885-2"),
  "Potassium" = c("2823-3"),
  "Albumin" = c("1751-7"),
  "Red blood" = c("26453-1"),
  "Bilirubin" = c("42719-5"),
  "Alkaline" = c("6768-6")
)

# Parcourir les correspondances et mettre à jour le mapping
for (concept_name in names(concept_mapping)) {
  matching_items <- grep(concept_name, mimic_info$label, ignore.case = TRUE, value = TRUE)
  
  if (length(matching_items) > 0) {
    mimic_omop_mapping$concept_name_omop[mimic_omop_mapping$label_mimic %in% matching_items] <- concept_name
    mimic_omop_mapping$concept_code_omop[mimic_omop_mapping$label_mimic %in% matching_items] <- concept_mapping[[concept_name]]
    
    # Chercher le concept_id correspondant dans resconcept_filtered
    matching_concept_id <- resconcept_filtered$concept_id[resconcept_filtered$concept_code == concept_mapping[[concept_name]]]
    
    mimic_omop_mapping$concept_id_omop[mimic_omop_mapping$label_mimic %in% matching_items] <- matching_concept_id
  }
}

# Affichage des résultats
result <- df_mimic_labevents %>%
  left_join(mapping_table, by = "subject_id") %>%
  left_join(mimic_omop_mapping, by = c("itemid" = "item_id_mimic")) %>%
  mutate(
    measurement_id = as.integer(labevent_id),
    measurement_concept_id = as.integer(itemid),
    measurement_date = as.Date(charttime),
    measurement_datetime = as.POSIXct(charttime),
    measurement_time = as.character(charttime),
    measurement_type_concept_id = as.integer(NA), #Y'a 5001 partout dans broadsea
    operator_concept_id = 0,
    value_as_number = as.double(valuenum),
    value_as_concept_id = 0,
    unit_concept_id = 0,
    range_low = as.double(ref_range_lower),
    range_high = as.double(ref_range_upper),
    provider_id = as.integer(NA),
    visit_occurrence_id = as.integer(hadm_id),
    visit_detail_id = as.integer(NA),
    measurement_source_value = as.character(concept_code_omop),
    measurement_source_concept_id = as.integer(concept_id_omop),
    unit_source_value = as.character(valueuom),
    value_source_value = as.character(value)
  ) %>%
  select(
    measurement_id, person_id, measurement_concept_id,
    measurement_date, measurement_datetime, measurement_time,
    measurement_type_concept_id, operator_concept_id, value_as_number,
    value_as_concept_id, unit_concept_id, range_low, range_high,
    provider_id, visit_occurrence_id, visit_detail_id,
    measurement_source_value, measurement_source_concept_id,
    unit_source_value, value_source_value
  )

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_measurement", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_measurement <- dbSendQuery(con, "SELECT * FROM cdm_measurement;")
fetch(df_cdm_measurement, n=-1)

# Déconnexion
dbDisconnect(con)
