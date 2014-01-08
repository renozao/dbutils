\name{dbManager}
\alias{dbManager}
\title{Database Connection Manager}
\usage{
  dbManager(..., max = 20, timeout = 10, cache = TRUE,
    verbose = FALSE, where = topenv(parent.frame()))
}
\arguments{
  \item{...}{database connection specification(s)}

  \item{max}{maximum number of connection attempts
  (integer)}

  \item{timeout}{time (in seconds) after which an error is
  thrown if the connection could not be opened. Within this
  period of time, \code{max} attempts are made, i.e. one
  every \code{timeout/max} seconds.}

  \item{cache}{logical that indicates if the last open
  connection should be returned if still valid.}

  \item{verbose}{verbosity level}

  \item{where}{namespace of the calling package (not to be
  used)}
}
\description{
  Connects to a database, possibly after multiple tries,
  e.g., if the number of concurrent connections hit its
  limit.
}
\details{
  This function is a wrapper around
  \code{\link{dbConnect}}, which provides the following
  extra features:

  \describe{ \item{Re-try with timeout}{useful when running
  many batch array jobs, which query a database that have a
  limited number of concurrent connection.}
  \item{Caching}{the last opened connection is cached and
  returned, if still valid} }
}

