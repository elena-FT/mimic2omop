library(RPostgreSQL)

# Fonction pour obtenir la liste des tables dans un schéma spécifique
# Paramètres :
#   - con : Connexion à la base de données
#   - schema : Nom du schéma à rechercher
# Sortie :
#   - Liste des noms de table dans le schéma spécifié
getTablesInSchema <- function(con, schema) {
  query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '", schema, "';")
  tables <- dbGetQuery(con, query)$table_name
  return(tables)
}

# Fonction pour trouver le schéma d'une table spécifique
# Paramètres :
#   - con : Connexion à la base de données
#   - tableName : Nom de la table à rechercher
# Sortie :
#   - Nom du schéma où la table est trouvée (ou NULL si non trouvée)
getSchemaForTable <- function(con, tableName) {
  query <- paste0("SELECT table_schema FROM information_schema.tables WHERE table_name = '", tableName, "';")
  schema <- dbGetQuery(con, query)$table_schema
  if (!is.null(schema)) {
    print(paste("La table", tableName, "se trouve dans le schéma", schema))
  } else {
    print(paste("La table", tableName, "n'a pas été trouvée dans les schémas existants."))
  }
  return(schema)
}

# Fonction pour récupérer les données depuis une table spécifique dans une base de données
# Paramètres :
#   - con : Connexion à la base de données
#   - tableName : Nom de la table à extraire
#   - numRows : Nombre de lignes à extraire (par défaut : 10)
# Sortie :
#   - DataFrame contenant les données de la table spécifiée
getDataFromTable <- function(con, tableName, numRows = 10) {
  query <- paste0("SELECT * FROM ", tableName, ";")
  result_db <- dbSendQuery(con, query)
  result <- dbFetch(result_db, n = numRows)
  dbClearResult(result_db)
  return(result)
}

# Fonction pour obtenir le nombre de connexions actives à la base de données
# Paramètres :
#   - connection : Connexion à la base de données
# Sortie :
#   - Nombre de connexions actives sous forme de DataFrame
getActiveConnections <- function(connection) {
  query <- "SELECT count(*) FROM pg_stat_activity;"
  active_connections <- dbGetQuery(connection, query)
  
  # Affichage du nombre de connexions actives
  print(active_connections)
}


# Afficher les tables dans le schéma demo_cdm
#tables_in_demo_cdm <- getTablesInSchema(con, "demo_cdm")
#print(tables_in_demo_cdm)

# Trouver le schéma qui possède la table source_daimon
#schema_for_source_daimon <- getSchemaForTable(con, "source_daimon")

# Récupérer les données depuis la table source_daimon
#source_daimon_data <- getDataFromTable(con, "webapi.source_daimon", numRows = 10)

# Récupérer les données depuis la table source
#source_data <- getDataFromTable(con, "webapi.source", numRows = 10)
