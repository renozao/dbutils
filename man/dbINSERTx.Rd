\name{dbINSERTx}
\alias{dbINSERTx}
\title{Insert Multiple Records into an SQL Table}
\usage{
  dbINSERTx(con, x, .table, .verbose = FALSE,
    .dump = FALSE)
}
\arguments{
  \item{x}{\code{data.frame} object, whose column names
  correspond to field names, and rows to the records to be
  inserted.}

  \item{.dump}{logical or character vector that indicates
  if the insertion query should be only dumped into a
  file.}

  \item{con}{SQL database connection object}

  \item{.table}{name of the table into which the recored is
  inserted}

  \item{.verbose}{verbose level}
}
\description{
  Inserts multiple data records into an SQL table directly
  from a \code{data.frame}.
}
\examples{
# connect to sample database
con <- .dbSampleConnection()
dbGetQuery(con, 'SELECT * FROM test')

# insert single record
df <- data.frame(a = 1:3, b = letters[1:3], c = c(1.5, NA, 6.5))
id <- dbINSERTx(con, df, .table = 'test')
id

# query table again
dbGetQuery(con, 'SELECT * FROM test')

# clean up
dbEMPTY(con, 'test')
}

