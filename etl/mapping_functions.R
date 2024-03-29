# mapping_functions.R

specimen_concept_mapping <- function(mimic_info, concept_filtered) {
  mapping_df <- data.frame(
    micro_specimen_id = integer(),
    spec_stype_desc = character(),
    specimen_concept_id = integer(),
    specimen_type_concept_id = integer(),
    stringsAsFactors = FALSE
  )
  
  for (spec_type_desc in c("SWAB", "URINE", "SEROLOGY/BLOOD", "STOOL", "IMMUNOLOGY",
                           "Blood (EBV)", "Blood (CMV AB)", "BRONCHIAL WASHINGS",
                           "BRONCHOALVEOLAR LAVAGE", "Mini-BAL", "FLUID,OTHER",
                           "Blood (Toxo)", "Influenza A/B by DFA", "MRSA SCREEN",
                           "Immunology (CMV)", "Staph aureus swab", "BLOOD CULTURE",
                           "FLUID RECEIVED IN BLOOD CULTURE BOTTLES", "Stem Cell - Blood Culture",
                           "Rapid Respiratory Viral Screen & Culture", "Blood (LYME)",
                           "Infection Control Yeast", "TISSUE", "PLEURAL FLUID",
                           "SPUTUM", "PERITONEAL FLUID", "ABSCESS", "FOREIGN BODY",
                           "CSF;SPINAL FLUID", "BLOOD CULTURE ( MYCO/F LYTIC BOTTLE)",
                           "JOINT FLUID", "BRONCHIAL BRUSH", "EAR", "CATHETER TIP-IV",
                           "FOOT CULTURE")) {
    
    # Trouver le concept correspondant dans concept_filtered
    print(spec_type_desc)
    matching_concept <- concept_filtered[grep(spec_type_desc, concept_filtered$concept_name, ignore.case = TRUE, value = TRUE), ][1, ]
    print(matching_concept)
    if (!is.null(matching_concept)) {
      new_row <- data.frame(
        micro_specimen_id_mimic = mimic_info[mimic_info$spec_type_desc == spec_type_desc, "micro_specimen_id"],
        spec_type_desc = spec_type_desc,
        specimen_concept_id = matching_concept$concept_id,
        specimen_type_concept_id = NA
      )
      mapping_df <- bind_rows(mapping_df, new_row)
    }
  }
  
  return(mapping_df)
}

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



procedure_concept_mapping <- function(mimic_data, cdm_data) {
  # Initialisez un dictionnaire vide
  mapping_table <- data.frame(
    item_id_mimic = mimic_data$itemid,
    label_mimic = mimic_data$label,
    abbreviation_mimic = mimic_data$abbreviation,
    concept_id_cdm = NA,
    concept_name_cdm = NA,
    concept_type_id_cdm = NA,
    concept_source_value = NA
  )
  
  # Correspondances basées sur les libellés
  label_mapping <- list(
    "20 Gauge" = "281325001",
    "Chest X-Ray" = "399208008",
    "18 Gauge" = "281326000",
    "Arterial Line" = "243144002",
    "EKG" = "69905003",
    "Invasive Ventilation" = "446971000124100",
    "CT scan" = "418602003",
    "Extubation" = "171207006",
    "Multi Lumen" = "246561000000100"
  )
  
  # Parcourir les correspondances et mettre à jour le mapping
  for (label_mimic in names(label_mapping)) {
    matching_items <- grep(label_mimic, mapping_table$label_mimic, ignore.case = TRUE, value = TRUE)
    if (length(matching_items) > 0) {
      matching_code <- label_mapping[[label_mimic]]
      matching_cdm <- resconcept_filtered[resconcept_filtered$concept_id == matching_code | resconcept_filtered$concept_code == matching_code, ]
      mapping_table$concept_source_value[mapping_table$label_mimic == label_mimic] <- matching_code
      
      if (nrow(matching_cdm) > 0) {
        indices_mapping_table <- match(matching_items, mapping_table$label_mimic)
        mapping_table$concept_id_cdm[indices_mapping_table] <- matching_cdm$concept_id
        mapping_table$concept_name_cdm[indices_mapping_table] <- matching_cdm$concept_name
        mapping_table$concept_type_id_cdm[indices_mapping_table] <- 38000275
        mapping_table$concept_source_value[indices_mapping_table] <- matching_code
      }
    }
  }
  
  return(mapping_table)
}

observation_concept_mapping <- function(mimic_info, concept_filtered) {
  # Initialisez un dictionnaire vide
  mimic_omop_mapping <- data.frame(
    item_id_mimic = mimic_info$itemid,
    label_mimic = mimic_info$label,
    abbreviation_mimic = mimic_info$abbreviation,
    cdm_concept_id = NA,
    cdm_concept_code = NA
  )
  
  # Créer la liste de correspondances à partir de concept_filtered
  label_mapping <- setNames(concept_filtered$concept_id, concept_filtered$concept_name)
  
  # Parcourir les correspondances et mettre à jour le mapping
  for (label_mimic in names(label_mapping)) {
    matching_items <- grep(label_mimic, mimic_info$label, ignore.case = TRUE, value = TRUE)
    if (length(matching_items) > 0) {
      mimic_omop_mapping$cdm_concept_id[mimic_omop_mapping$label_mimic %in% matching_items] <- label_mapping[[label_mimic]]
      mimic_omop_mapping$cdm_concept_code[mimic_omop_mapping$label_mimic %in% matching_items] <- label_mimic
    }
  }
  
  return(mimic_omop_mapping)
}