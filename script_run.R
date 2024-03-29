#script_run.R
library(DBI)

source("etl/usefull_fonctions.R")
source("etl/cdm_person.R")
source("etl/cdm_death.R")
source("etl/cdm_drug_exposure.R")
source("etl/cdm_measurement.R")
source("etl/cdm_procedure_occurrence.R")
source("etl/cdm_specimen.R")
source("etl/cdm_visit_occurrence.R")
source("etl/cdm_condition_occurrence.R")

con <- connect_broadsea()
dbListTables(con)


# cdm_person - Requête SQL pour récupérer toutes les femmes
query <- "SELECT * FROM cdm_person WHERE gender_concept_id = 8532"
women <- dbGetQuery(con, query)

# cdm_measurement - Requete SQL pour récupérer la mesure de l'hemoglobin

query_total <- 'SELECT COUNT(*) FROM cdm_measurement'
total_measurements <- dbGetQuery(con, query_total)$count

query_hemoglobin <- 'SELECT COUNT(*) FROM cdm_measurement WHERE measurement_source_value = \'718-7\''

hemoglobin_measurements <- dbGetQuery(con, query_hemoglobin)$count
percentage_hemoglobin <- (hemoglobin_measurements / total_measurements) * 100
cat(sprintf("Pourcentage de mesures d'hémoglobine par rapport à toutes les mesures : %.2f%%\n", percentage_hemoglobin))

query <- "SELECT person_id, COUNT(*) AS nombre_prises_hemoglobine
          FROM cdm_measurement
          WHERE measurement_source_value = \'718-7\'
          GROUP BY person_id
          ORDER BY person_id
          LIMIT 1;"

# Exécution de la requête
result <- dbGetQuery(con, query)

# cdm_procedure - Requete pour voir quel patient a fait le plus de CT-Scan

query_max_ct_scans <- "SELECT person_id, COUNT(*) AS nombre_procedures_CT_scan
                       FROM cdm_procedure_occurrence
                       WHERE procedure_source_value = '418602003'
                       GROUP BY person_id
                       ORDER BY COUNT(*) DESC
                       LIMIT 1;"

result_max_ct_scans <- dbGetQuery(con, query_max_ct_scans)

# cdm_drug 

query <- "SELECT
  p.person_id,
  COUNT(de.drug_exposure_id) AS nombre_aspirines
FROM
  cdm_person p
JOIN
  cdm_condition_occurrence co ON p.person_id = co.person_id
JOIN
  cdm_drug_exposure de ON p.person_id = de.person_id
WHERE
  co.condition_concept_id = 313217 -- Condition : Fibrillation auriculaire
  AND de.drug_concept_id = 1112807 -- Médicament : Aspirine
GROUP BY
  p.person_id
ORDER BY
  nombre_aspirines DESC;
"

# Exécution de la requête
result <- dbGetQuery(con, query)

dbDisconnect(con)
