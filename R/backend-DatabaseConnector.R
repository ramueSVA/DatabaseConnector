#' #' @export sql_values_subquery
#' #' @export
#' sql_values_subquery <- function(con, df, types, lvl = 0, ...){
#'   check_dots_used()
#'   UseMethod("sql_values_subquery")
#' }
#' 
#' #' @export sql_values_subquery.DatabaseConnectorJdbcConnection
#' #' @export
#' sql_values_subquery.DatabaseConnectorJdbcConnection <- function(con, df, types, lvl = 0, ...) {
#'   if(dbms(con) == "redshift"){
#'     return(utils::getFromNamespace("sql_values_subquery.RedshiftConnection", "dbplyr")(con = con, df = df, types = types, lvl = lvl, ...))
#'   }
#'   else{
#'     return(utils::getFromNamespace("sql_values_subquery.DBIConnection", "dbplyr")(con = con, df = df, types = types, lvl = lvl, ...))
#'   }
#' }


#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}

#' @export
#' @importFrom dbplyr sql_query_select
sql_query_select.DatabaseConnectorJdbcConnection <- function(con,
                                                             select,
                                                             from,
                                                             where = NULL,
                                                             group_by = NULL,
                                                             having = NULL,
                                                             window = NULL,
                                                             order_by = NULL,
                                                             limit = NULL,
                                                             distinct = FALSE,
                                                             ...,
                                                             subquery = FALSE,
                                                             lvl = 0) {
  switch(dbms(con),
    "sql server" = utils::getFromNamespace("sql_query_select.Microsoft SQL Server", "dbplyr")(con,
      select,
      from,
      where,
      group_by,
      having,
      window,
      order_by,
      limit,
      distinct,
      ...,
      subquery,
      lvl),
    "oracle" = utils::getFromNamespace("sql_query_select.Oracle", "dbplyr")(con,
      select,
      from,
      where,
      group_by,
      having,
      window,
      order_by,
      limit,
      distinct,
      ...,
      subquery,
      lvl),
    NextMethod()  
  )
}

#' @export
#' @importFrom dbplyr sql_translation 
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  
  switch(dbms(con),
     "oracle" = utils::getFromNamespace("sql_translation.Oracle", "dbplyr")(con),
     "postgresql" = utils::getFromNamespace("sql_translation.PqConnection", "dbplyr")(con),
     "redshift" = utils::getFromNamespace("sql_translation.RedshiftConnection", "dbplyr")(con),
     "sql server" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     "bigquery" = utils::getFromNamespace("sql_translation.BigQueryConnection", "bigrquery")(con),
     "spark" = utils::getFromNamespace("sql_translation.Spark SQL", "dbplyr")(con),
     "snowflake" = utils::getFromNamespace("sql_translation.Snowflake", "dbplyr")(con),
     "synapse" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     "iris" = utils::getFromNamespace("sql_translation.PqConnection", "dbplyr")(con),
     rlang::abort("Sql dialect is not supported!")) 
}
