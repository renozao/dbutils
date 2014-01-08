# Project: dbutils
# 
# Author: Renaud Gaujoux
# Created: Nov 18, 2013
###############################################################################


#' Check Connection Validity
#' 
#' \code{dbIsValid} tests if a connection is valid, i.e. open.
#' 
#' @param con connection object, as returned by \code{\link{dbConnect}}.
#' @export
#' 
#' 
dbIsValid <- function(con){
    !is( try( dbGetInfo(con), silent = TRUE), 'try-error' )
}

#' SQL Database Connection Utilities
#' 
#' \code{dbDisconnectAll} closes all database connections for a given driver.
#' 
#' @param drv database driver
#' @export
#' @rdname con-utils
dbDisconnectAll <- function(drv){
	
    if( is.function(drv) ) drv <- drv()
	# disconnect
	con <- lapply(dbListConnections(drv), dbDisconnect)
    # return number of disonnected connections 
	length(con)
}


#' @param con SQL database connection object
#' @export
#' @rdname con-utils
dbCOMMIT <- function(con){
    rs <- dbSendQuery(con, 'COMMIT;')
    dbClearResult(rs)
}

#' @export
#' @rdname con-utils
dbROLLBACK <- function(con){
    rs <- dbSendQuery(con, 'ROLLBACK;')
    dbClearResult(rs)
}

#' @export
#' @rdname con-utils
dbTRANSACTION <- function(con){
    rs <- dbSendQuery(con, 'START TRANSACTION;')
    dbClearResult(rs)
}


#' Database Selection Generator
#' 
#' Generates functions that extract SQL database record fields based on 
#' the value of a given key. 
#'  
#' @param con SQL database connection object
#' @export
dbSelector <- function(con){
    
    .con <- con
    function(table, key, select = '*'){
        
        function(id, con = .con, each = TRUE){
            
            # first execute connection function if necessary
            if( is.function(con) ) con <- con()
            
            .local <- function(id){
                sql <- sprintf("SELECT %s FROM %s WHERE %s", select, table, .sql_criteria(key, id))
                res <- dbGetQuery(con, sql)
                if( !nrow(res) ) NA
                else if( ncol(res) == 1L ) res[[1L]]
                else res
            }
            if( each ){
                res <- sapply(id, .local)
                if( any(notfound <- is.na(res)) ) 
                    stop("Not all entry ids were found [", str_out(id[notfound], total = TRUE), ']')
            }else{
                res <- .local(id)
            }
            if( is.null(dim(res)) ) res <- as.character(res)
            res
            
        }
    }
}


