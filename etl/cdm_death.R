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
dbExecute(con, "DROP TABLE IF EXISTS cdm_death;")

death <- dbSendQuery(con, "SELECT * FROM demo_cdm.death;")

# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
admission_file <- file.path(mimic_folder, "hosp", "admissions.csv.gz")
df_mimic_admissions <- fread(admission_file)

col_death_info <- dbColumnInfo(death)
col_cdm_death_info <- paste0(
  paste0(col_death_info$name, " ", col_death_info$type, " ", ifelse(col_death_info$nullable, "NULL", "NOT NULL")),
  collapse = ",\n"
)

query_create_table <- paste0(
  "CREATE TABLE cdm_death (\n",
  col_cdm_death_info, "\n);"
)

dbClearResult(death)
# À exécuter qu'une fois (pour créer la table)
dbExecute(con, query_create_table)

# Affichage des résultats
result <- df_mimic_admissions %>%
  left_join(mapping_table, by = "subject_id") %>%
  mutate(
    death_date = deathtime,
    death_datetime = deathtime,
    death_type_concept_id = as.integer(NA),
    cause_concept_id = as.integer(NA),
    cause_source_value = as.character(NA),
    cause_source_concept_id = as.integer(NA)
  ) %>%
  select(
    person_id, death_date, death_datetime, 
    death_type_concept_id, cause_concept_id, cause_source_value, 
    cause_source_concept_id
  )

# Afficher le résultat
print(result)

# Écrire les résultats dans la table cdm_death
dbWriteTable(con, "cdm_death", result, append = TRUE, row.names = FALSE)

# Afficher les données de la table cdm_death
df_cdm_death <- dbSendQuery(con, "SELECT * FROM cdm_death;")
fetch(df_cdm_death, n=-1)

dbDisconnect(con)