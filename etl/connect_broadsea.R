# connect_broadsea.R

connect_broadsea <- function() {
  dsn_database <- "postgres"
  dsn_hostname <- "broadsea-atlasdb"
  dsn_port <- "5432"
  dsn_uid <- "postgres"
  dsn_pwd <- "mypass"
  
  tryCatch({
    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv,
                     dbname = dsn_database,
                     host = dsn_hostname,
                     port = dsn_port,
                     user = dsn_uid,
                     password = dsn_pwd)
    print("Connection réussie !")
    return(con)
  })
}
