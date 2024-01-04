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

procedure_concept_mapping <- function(mimic_info, concept_filtered) {
  # Initialisez un dictionnaire vide
  mimic_omop_mapping <- data.frame(
    item_id_mimic = mimic_info$itemid,
    abbrevation_mimic = mimic_info$abbreviation,
    concept_type_id_omop = NA,
    concept_source_value_omop = NA,
    concept_id_omop = NA
  )
  
  # Correspondances abbrevation_mimic et concept_code
  concept_mapping <- list(
    "Dialysis" = c("718-7"),
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
    matching_items <- grep(concept_name, mimic_info$abbreviation, ignore.case = TRUE, value = TRUE)
    
    if (length(matching_items) > 0) {
      for (matching_item in matching_items) {
        mimic_omop_mapping$concept_id_omop[matching_item] <- concept_mapping[[concept_name]]
        mimic_omop_mapping$concept_type_id_omop[matching_item] <- concept_filtered$concept_code[concept_filtered$concept_id == concept_mapping[[concept_name]]]
        mimic_omop_mapping$concept_source_value_omop[matching_item] <- mimic_omop_mapping$concept_type_id_omop[matching_item]
      }
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
    "Dialysis - SCUF" = "Peritoneal Dialysis",
    "Dialysis - CVVHD" = "Hemodialysis",
    "Dialysis - CRRT" = "Hemodialysis",
    "CRRT Filter Change" = "Hemodialysis",
    "Dialysis - CVVHDF" = "Hemodialysis",
    "Ultrasound" = "Ultrasound",
    "Travel to Radiology" = "Radiography of ankle",
    "Portable Chest X-Ray" = "Radiography",
    "Cervical Spine" = "Imaging",
    "Trascranial Doppler" = "Imaging",
    "Venogram" = "Imaging",
    "Portable CT scan" = "Imaging",
    "Transthoracic Echo" = "Imaging",
    "Chest X-Ray" = "Imaging",
    "Angiography" = "Imaging",
    "Pelvis" = "Imaging",
    "Abdominal X-Ray" = "Imaging",
    "TEE" = "Imaging"
  )
  
  # Parcourir les correspondances et mettre à jour le mapping
  for (label_mimic in names(label_mapping)) {
    print(label_mimic)
    matching_items <- grep(label_mimic, mimic_data$label, ignore.case = TRUE, value = TRUE)
    if (length(matching_items) > 0) {
      matching_cdm <- cdm_data[cdm_data$concept_name %in% label_mapping[[label_mimic]], ]
      if (nrow(matching_cdm) > 0) {
        mapping_table$concept_id_cdm[mapping_table$label_mimic %in% matching_items] <- matching_cdm$concept_id
        mapping_table$concept_name_cdm[mapping_table$label_mimic %in% matching_items] <- matching_cdm$concept_name
        mapping_table$concept_type_id_cdm[mapping_table$label_mimic %in% matching_items] <- matching_cdm$concept_code
        mapping_table$concept_source_value[mapping_table$label_mimic %in% matching_items] <- matching_cdm$concept_id
      }
    }
  }
  
  return(mapping_table)
}
