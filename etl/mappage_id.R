# Récupération des données de Mimic IV Démo
mimic_folder <- "./mimic-iv-clinical-database-demo-2.2"
person_file <- file.path(mimic_folder, "hosp", "patients.csv.gz")
df_mimic_person <- fread(person_file)

mapping_table <- df_mimic_person %>%
  select(subject_id) %>%
  arrange(subject_id) %>%
  mutate(person_id = row_number())

