# Project: dbutils
# 
# Author: Renaud Gaujoux
# Created: Jan 8, 2014
###############################################################################

isString <- function(x) is.character(x) && length(x) == 1L

#' Database Connection Manager
#' 
#' Connects to a database, possibly after multiple tries, 
#' e.g., if the number of concurrent connections hit its limit.
#' 
#' This function is a wrapper around \code{\link{dbConnect}}, 
#' which provides the following extra features:
#' 
#' \describe{
#' \item{Re-try with timeout}{useful when running many batch array jobs,
#' which query a database that have a limited number of concurrent connection.}
#' \item{Caching}{the last opened connection is cached and returned, if still valid}
#' }
#' 
#' @param dbname database connection specification(s)
#' @param max maximum number of connection attempts (integer)
#' @param timeout time (in seconds) after which an error is thrown if the connection could not be 
#' opened.
#' Within this period of time, \code{max} attempts are made, i.e. one every \code{timeout/max} seconds.
#' @param cache logical that indicates if the last open connection should be returned if still valid. 
#' @param verbose verbosity level
#' @param where namespace of the calling package (not to be used)
#' 
#' @export
#' @import DBI
#' @importFrom utils packageName
dbManager <- local({
            
        # cached connection
        .db <- list()
        function(dbname, max = 20, timeout = 10, cache = TRUE, verbose = FALSE, where = topenv(parent.frame())){
            
                dbkey <- dbname
                # early exit if not a string
                if( !isString(dbname) ) return(dbname)
                # check previous connection
                if( cache && !is.null(con <- .db[[dbkey]]) ){
                    # return cached connection object if still valid
                    if( dbIsValid(con) ) return(con)
                }
                # reset old connection object (do not close the connection)
                .db[[dbkey]] <<- NULL
                
                # detect driver
                if( grepl("^([^:]+):", dbname) ){
                    drvName <- gsub("^([^:]+):.*", "\\1", dbname)
                    dbname <- gsub("^([^:]+):", "", dbname)
                }else{
                    drvName <- 'SQLite'
                    if( grepl("^@", dbname) ){# use db in package
                        dbname <- system.file('db', paste0(gsub("^@", '', dbname), '.sqlite3'), package = packageName(where))
                    }
                }
                library(pkg <- paste0('R', drvName), character.only = TRUE)
                drv <- getFunction(drvName, where = paste0('package:', pkg))
                
                if( verbose ) message("Opening new connection to ", drvName, ":", dbname, ' ... ', appendLF = FALSE)
                # try-connection loop
                .try <- 1
                while( .try <= max ){
                        
                    con <- tryCatch({
                            if( drvName == 'MySQL' ){
                                    dbConnect(drv(), group = dbname)
                            }else dbConnect(drv(), dbname)
                        }
                        , error = function(e){
                            if( .try >= max ){
                                    stop("Timeout after ", max, " attempts of connection to ", drvName,":", dbname, " - ", e)
                            }
                            Sys.sleep( timeout / max )
                        }) 
            
                    if( !is.null(con) ) break;
                    .try <- .try + 1
                }
                if( verbose ) message('OK [', .try, ']')
                
                # cache and return connection object 
                .db[[dbkey]] <<- con
                con
        }
})

# Tests parallel access to a MySQL database
# To really see if the retry/timeout approach works,
# one first needs to set the system variable to something low
# max-connections = 10
testCon <- function(){
    
    library(parallel)
    res <- mclapply(1:20, function(i, ...){
                    message("Thread ", i)
                    db <- dbManager('MySQL:testdb')
                    Sys.sleep(1)
                    dbDisconnect(db)
                    
            }, mc.cores = 20)
    
}

