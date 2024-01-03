# mapping_functions.R

measurement_concept_mapping <- function(mimic_info, resconcept_filtered) {
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
  
  return(mimic_omop_mapping)
}
