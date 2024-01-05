library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)
library(digest)
library(stringr)


# Charger le fichier de connexion
source("etl/connect_broadsea.R")
source("etl/mappage_id.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Supprimer la table cdm_death si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_drug_exposure;")

drug_exposure <- dbSendQuery(con, "SELECT * FROM demo_cdm.drug_exposure;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
prescriptions_file <- file.path(mimic_folder, "hosp", "prescriptions.csv.gz")
df_mimic_prescriptions <- fread(prescriptions_file)

col_drug_exposure_info <- dbColumnInfo(drug_exposure)
col_cdm_drug_exposure_info <- paste0(
  paste0(col_drug_exposure_info$name, " ", col_drug_exposure_info$type, " ", ifelse(col_drug_exposure_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_drug_exposure (\n",
  col_cdm_drug_exposure_info, "\n);"
)

dbClearResult(drug_exposure)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

concept <- dbSendQuery(con, "SELECT * FROM demo_cdm.concept;")
resconcept <- dbFetch(concept, -1)
dbClearResult(concept)

calculate_average <- function(value) {
  if (value == "") {
    return(0)
  }
  if (grepl("-", value)) {
    range_values <- as.numeric(strsplit(value, "-")[[1]])
    average <- mean(range_values, na.rm = TRUE)
    return(average)
  } else {
    return(as.numeric(value))
  }
}

correspondance_medicaments <- data.frame(
  medicament = c("Sodium Chloride", "Magnesium Sulfate", "Bag", "Potassium Chloride", "Heparin", "Insulin", "Calcium Gluconate"),
  formulary_drug = c("NACLFLUSH", "MAG2PM", "BAG50", "MICROK10", "HEPA5I", "GLAR100I", "CALCG2/100NS"),
  concept_id = NA,
  concept_code = NA
)

correspondance_medicaments <- correspondance_medicaments %>%
  mutate(
    concept_id = sapply(medicament, function(med) {
      matching_concept <- resconcept$concept_id[grep(med, resconcept$concept_name, ignore.case = TRUE)[1]]
      if (!is.na(matching_concept)) {
        return(matching_concept)
      } else {
        return(NA)
      }
    }),
    concept_code = sapply(medicament, function(med) {
      matching_source_value <- resconcept$concept_code[grep(med, resconcept$concept_name, ignore.case = TRUE)[1]]
      if (!is.na(matching_source_value)) {
        return(matching_source_value)
      } else {
        return(NA)
      }
    })
  )


result <- df_mimic_prescriptions %>%
  left_join(mapping_table, by = "subject_id") %>%
  mutate(
    drug_exposure_id = row_number(),
    drug_concept_id = correspondance_medicaments$concept_id[match(formulary_drug_cd, correspondance_medicaments$formulary_drug)],
    drug_exposure_start_date = as.Date(starttime),
    drug_exposure_start_datetime = as.POSIXct(starttime),
    drug_exposure_end_date = as.Date(stoptime),
    drug_exposure_end_datetime = as.POSIXct(stoptime),
    verbatim_end_date = as.Date(NA),
    drug_type_concept_id = 38000177, #Clinical Drug ou Branded Drug (581452) à revoir
    stop_reason = as.character(NA),
    refills = 0,
    quantity = sapply(df_mimic_prescriptions$form_val_disp, calculate_average),
    days_supply = as.integer(NA),
    sig = as.character(NA),
    route_concept_id = as.integer(NA),
    lot_number = as.character(NA),
    provider_id = 0,
    visit_occurrence_id = as.integer(NA),
    visit_detail_id = 0,
    drug_source_value = correspondance_medicaments$concept_code[match(formulary_drug_cd, correspondance_medicaments$formulary_drug)],
    drug_source_concept_id = correspondance_medicaments$concept_id[match(formulary_drug_cd, correspondance_medicaments$formulary_drug)],
    route_source_value = route,
    dose_unit_source_value = form_unit_disp
  ) %>%
  select(
    drug_exposure_id, person_id, drug_concept_id, 
    drug_exposure_start_date, drug_exposure_start_datetime,
    drug_exposure_end_date, drug_exposure_end_datetime,
    verbatim_end_date, drug_type_concept_id,
    stop_reason, refills, quantity, days_supply,
    sig, route_concept_id, lot_number, provider_id,
    visit_occurrence_id, visit_detail_id, drug_source_value,
    drug_source_concept_id, route_source_value, dose_unit_source_value
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_drug_exposure", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_drug_exposure <- dbSendQuery(con, "SELECT * FROM cdm_drug_exposure;")
fetch(df_cdm_drug_exposure, n=-1)

dbDisconnect(con)