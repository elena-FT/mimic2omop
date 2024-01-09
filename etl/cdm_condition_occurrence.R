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
dbClearResult(condition_occurrence)

# Supprimer la table cdm_condition_occurrence si elle existe déjà
dbExecute(con, "DROP TABLE IF EXISTS cdm_condition_occurrence;")

# A executer qu'une fois (pour creer la table)
dbExecute(con, paste0(
  "CREATE TABLE cdm_condition_occurrence (\n",
  col_cdm_co_info, "\n);"
))


# Récupère les tables de concepts
resconcept <- getDataFromTable(con, "demo_cdm.concept", -1)
res_concept_relationship <- getDataFromTable(con, "demo_cdm.concept_relationship", -1)

# Simplifies some desease to mappe to OMOP
df_mimic_diagnoses_d <- df_mimic_diagnoses_d %>%
  mutate(concept_name = case_when(
    grepl("pneumonia", tolower(long_title)) ~ "Pneumonia",
    grepl("epilepsy", tolower(long_title)) ~ "Epilepsy",
    grepl("esophagitis", tolower(long_title)) ~ "Esophagitis",
    grepl("osteoarthritis", tolower(long_title)) ~ "Osteoarthritis",
    grepl("gallstone", tolower(long_title)) ~ "Gallstone",
    grepl("pyelonephritis", tolower(long_title)) ~ "Pyelonephritis",
    grepl("anemia", tolower(long_title)) ~ "Anemia",
    grepl("appendicitis", tolower(long_title)) ~ "Appendicitis",
    TRUE ~ long_title
  )) %>%
  select(icd_code, concept_name)


# Crée la table de mapping pour les concept_id
mapping_table_condition <- df_mimic_diagnoses_d %>%
  inner_join(resconcept, by = "concept_name") %>%
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
    condition_occurrence_id,
    person_id,
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
getDataFromTable(con, "cdm_condition_occurrence", -1)

requete_person <- dbGetQuery(con, "SELECT DISTINCT person_id FROM cdm_condition_occurrence WHERE condition_concept_id = 313217")

requete_person_visit <- dbSendQuery(con, "SELECT
    co.person_id,
    CASE WHEN p.gender_concept_id = 8532 THEN 'F' 
         WHEN p.gender_concept_id = 8507 THEN 'M' 
         ELSE 'Unknown' 
    END AS gender,
    SUM(vo.visit_end_date - vo.visit_start_date) AS total_time_spent_in_hospital
FROM
    cdm_condition_occurrence co
JOIN
    cdm_visit_occurrence vo ON co.visit_occurrence_id = vo.visit_occurrence_id
JOIN
    cdm_person p ON co.person_id = p.person_id
WHERE
    co.condition_concept_id = 313217
GROUP BY
    co.person_id, p.gender_concept_id;
;
")
res_requete_person_visit <- fetch(requete_person_visit, n=-1)
dbClearResult(requete_person_visit)

requete_person_visit_stat <- dbSendQuery(con, "SELECT
    gender,
    COUNT(DISTINCT person_id) AS total_cases,
    AVG(time_spent) AS average_time_spent_in_hospital,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_spent) AS median_time_spent_in_hospital
FROM (
    SELECT
        CASE WHEN p.gender_concept_id = 8532 THEN 'F' 
             WHEN p.gender_concept_id = 8507 THEN 'M' 
             ELSE 'Unknown' 
        END AS gender,
        co.person_id,
        SUM(vo.visit_end_date - vo.visit_start_date) AS time_spent
    FROM
        cdm_condition_occurrence co
    JOIN
        cdm_visit_occurrence vo ON co.visit_occurrence_id = vo.visit_occurrence_id
    JOIN
        cdm_person p ON co.person_id = p.person_id
    WHERE
        co.condition_concept_id = 313217
    GROUP BY
        gender,
        co.person_id
) AS subquery
GROUP BY
    gender;
")
res_requete_person_visit_stat <- fetch(requete_person_visit_stat, n=-1)
dbClearResult(requete_person_visit_stat)

dbDisconnect(con)
