# Project: dbutils
# 
# Author: Renaud Gaujoux
# Created: Jan 8, 2014
###############################################################################

#' SQL Special Types
#' 
#' These variables can be used in SQL statements defined by \code{\link{SQLquery}} objects. 
#' 
#' @rdname SQLtypes
#' @export
sql_NULL <- as.expression('NULL')
#' @rdname SQLtypes
#' @export
sql_NOW <- as.expression('NOW()')

#' Insert Single Record into an SQL Table
#' 
#' @param con SQL database connection object
#' @param ... named arguments used as pairs (field, value)
#' @param .table name of the table into which the recored is inserted
#' @param .verbose verbose level
#' 
#' @return the primary key of the newly inserted record
#' 
#' @examples
#' 
#' # connect to sample database
#' con <- .dbSampleConnection()
#' dbGetQuery(con, 'SELECT * FROM test')
#' 
#' # insert single record 
#' id <- dbINSERT(con, a = 1, b = 'blabla', c = NA, t = sql_NOW, .table = 'test')
#' id
#' 
#' # query table again
#' dbGetQuery(con, 'SELECT * FROM test')
#' 
#' # clean up
#' dbEMPTY(con, 'test')
#' 
#' @export
dbINSERT <- function(con, ..., .table, .verbose = FALSE){
	
    table_name <- .table
    sql <- SQLquery(table_name)
    sql$add(...)
    ins <- sql$insert_query()
    if( .verbose ) cat("\n", ins, "\n", sep='')
	rs <- dbSendQuery(con, ins)
	dbClearResult(rs)
	invisible(dbGetQuery(con, "SELECT LAST_INSERT_ID()")[1,1])
}

#' Insert Multiple Records into an SQL Table
#' 
#' Inserts multiple data records into an SQL table directly from a \code{data.frame}.
#' 
#' @inheritParams dbINSERT
#' @param x \code{data.frame} object, whose column names correspond to field names, and 
#' rows to the records to be inserted.
#' @param .dump logical or character vector that indicates if the insertion query should be 
#' only dumped into a file.  
#' 
#' @export
#' @examples 
#' # connect to sample database
#' con <- .dbSampleConnection()
#' dbGetQuery(con, 'SELECT * FROM test')
#' 
#' # insert single record
#' df <- data.frame(a = 1:3, b = letters[1:3], c = c(1.5, NA, 6.5)) 
#' id <- dbINSERTx(con, df, .table = 'test')
#' id
#' 
#' # query table again
#' dbGetQuery(con, 'SELECT * FROM test')
#' 
#' # clean up
#' dbEMPTY(con, 'test')
#' 
dbINSERTx <- function(con, x, .table, .verbose = FALSE, .dump = FALSE){
    
    dump <- .dump
    table_name <- .table
    sql <- SQLquery(table_name)
    qt <- system.time(query <- sql$minsert_query(mdata = x))
    # print(qt)
    if( isFALSE(dump) ){ # direct query
        
        rs <- dbSendQuery(con, query)
        dbClearResult(rs)
        invisible(dbGetQuery(con, "SELECT LAST_INSERT_ID()")[1,1])
    }else{ # dump into file and import
        tmp <- if( is.character(dump) ) dump 
               else{
                   tempfile(fileext = ".sql")
               }
        ft <- system.time(cat(query, file = tmp))
        invisible(tmp)
    }
}

#' @export
SQLquery <- function(table_name){
    
    .fields <- list()
    .mdata <- list()
    .table_name <- table_name
    .stick <- function(fields, sep = if( value.only ) ',' else '=', value.only = FALSE){
        ins <- sapply(names(fields), function(f){
                    v <- fields[[f]]
                    qv <- if( is.character(v) ) paste0('"', v, '"') 
                            else if( length(v) == 1L && (is.null(v) || is_NA(v)) ) sql_NULL
                            else as.character(v)
                    if( value.only ) qv
                    else paste0("`", f, "` ", sep, ' ', qv)
                }, simplify = !is.list(fields) || length(fields[[1]]) == 1L)
        
        if( value.only ){
            names(ins) <- NULL
            ins$sep = sep
            do.call(paste, ins)
        }else ins
    }
    
    
    .update_query <- function(id, table_name = .table_name, idfield = 'id'){
        sql <- .stick(.fields)
        sql <- paste0(sql, collapse = ", ")
        paste0('UPDATE `', table_name, "` SET ", sql
                , " WHERE `", idfield, "` = '", id, "';"
                , collapse = "\n")
    }
    
    .add_field <- function(name, value, escape = FALSE){
        
        # escape special characters if necessary
        if( escape ) value <- escape_sql(value)
        # add to field list
        .fields[[name]] <<- value 
    }
    
    .add <- function(..., escape = FALSE){
        fields <- list(...)
        if( length(fields) == 1L && is.null(names(fields)) )
            fields <- as.list(fields[[1]])
        lapply(seq_along(fields), function(i){
                    .add_field(names(fields)[i], fields[[i]], escape = escape)
                })
        length(fields)
    }
    
    list(
            size = function() length(.fields)
            , set_fields = function(...){
                fields <- list(...)
                if( length(fields) == 1L && is.null(names(fields)) )
                    fields <- as.list(fields[[1]])
                .fields <<- fields
            }
            , add = .add
            , add_field = .add_field
            , update_query = .update_query 
            , sendUpdate = function(con, ...){
                sql <- .update_query(...)
                rs <- dbSendQuery(con, sql)
                dbClearResult(rs)
            }
            , madd = function(...){
                if( length(.fields) ){
                    .mdata <<- c(.mdata, list(.fields))
                    .fields <<- list()
                }
                .add(...)
                .mdata <<- c(.mdata, list(.fields))
                .fields <<- list()
            }
            , minsert_query = function(table_name = .table_name, mdata = .mdata){
                sql <- .stick(mdata, value.only = TRUE)
                paste0("INSERT INTO `", table_name, "` ("
                        , paste0('`', names(mdata), '`', collapse = ",")
                        , ") VALUES ", paste0("(", sql, ")", collapse = ","))
            }
            , insert_query = function(table_name = .table_name){
                sql <- .stick(.fields)
                sql <- paste0(sql, collapse = ", ")
                sql <- paste0('INSERT INTO `', table_name, "` SET ", sql, ";"
                        , collapse = "\n")
            }
    )
}

