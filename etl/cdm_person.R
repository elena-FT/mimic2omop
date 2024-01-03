library(Hades)
library(SqlRender)
library(RPostgreSQL)
library(data.table)
library(tibble)
library(dplyr)

# Charger le fichier de connexion
source("connect_broadsea.R")
source("mappage_id.R")

# Connexion à la base de données de broadsea
con <- connect_broadsea()

# Afficher les tables dans la base de données
dbListTables(con)
person <- dbSendQuery(con, "SELECT * FROM demo_cdm.person;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
person_file <- file.path(mimic_folder, "hosp", "patients.csv.gz")
df_mimic_person <- fread(person_file)

col_person_info <- dbColumnInfo(person)
col_cdm_person_info <- paste0(
  paste0(col_person_info$name, " ", col_person_info$type, " ", ifelse(col_person_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE demo_cdm.cdm_person (\n",
  col_cdm_person_info, "\n);"
)

dbClearResult(person)
# A executer qu'une fois (pour creer la table)
dbExecute(con, query_create_table)

# Affichage des résultats
result <- df_mimic_person %>%
  left_join(mapping_table, by = "subject_id") %>%
  mutate(
    person_id_test = row_number(),
    gender_concept_id = case_when(
      gender == 'F' ~ 8532,
      gender == 'M' ~ 8507,
      TRUE ~ 0
    ),
    year_of_birth = anchor_year,
    month_of_birth = as.integer(NA),
    day_of_birth = as.integer(NA),
    birth_datetime = as.POSIXct(NA),
    race_concept_id = as.integer(NA),
    ethnicity_concept_id = as.integer(NA),
    location_id = as.integer(NA),
    provider_id = as.integer(NA),
    care_site_id = as.integer(NA),
    person_source_value = as.character(subject_id),
    gender_source_value = as.character(gender),
    gender_source_concept_id = 0,
    race_source_value = as.character(NA),
    race_source_concept_id = as.integer(NA),
    ethnicity_source_value = as.character(NA),
    ethnicity_source_concept_id = as.integer(NA)
  ) %>%
  select(
    person_id, gender_concept_id, year_of_birth, 
    month_of_birth, day_of_birth, birth_datetime, 
    race_concept_id, ethnicity_concept_id, location_id, 
    provider_id, care_site_id, person_source_value, 
    gender_source_value, gender_source_concept_id, 
    race_source_value, race_source_concept_id, 
    ethnicity_source_value, ethnicity_source_concept_id
  )

# Afficher le résultat
print(result)

# Supprimer la table cdm_person si elle existe déjà
#dbExecute(con, "DROP TABLE IF EXISTS cdm_person;")

# Écrire les résultats dans la table cdm_person
dbWriteTable(con, c("demo_cdm", "cdm_person"), result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_person
df_cdm_person <- dbSendQuery(con, "SELECT * FROM demo_cdm.cdm_person;")

fetch(df_cdm_person, n=-1)