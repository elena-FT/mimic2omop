#script_run.R

source("etl/usefull_fonctions.R")
source("etl/cdm_person.R")
source("etl/cdm_death.R")
source("etl/cdm_drug_exposure.R")
source("etl/cdm_measurement.R")
source("etl/cdm_procedure_occurrence.R")
source("etl/cdm_specimen.R")

con <- connect_broadsea()
dbListTables(con)
dbDisconnect(con)