\name{dbDisconnectAll}
\alias{dbCOMMIT}
\alias{dbDisconnectAll}
\alias{dbROLLBACK}
\alias{dbTRANSACTION}
\title{SQL Database Connection Utilities}
\usage{
  dbDisconnectAll(drv)

  dbCOMMIT(con)

  dbROLLBACK(con)

  dbTRANSACTION(con)
}
\arguments{
  \item{drv}{database driver}

  \item{con}{SQL database connection object}
}
\description{
  \code{dbDisconnectAll} closes all database connections
  for a given driver.
}

