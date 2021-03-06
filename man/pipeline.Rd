% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pipeline.R
\name{pipeline}
\alias{pipeline}
\title{Creates a pipeline out of a set of pipes}
\usage{
pipeline(...)
}
\arguments{
\item{...}{Pipes create with \code{\link{pipe}} or \code{\link{pipeline}}}
}
\value{
A pipeline object, which can be applied to new data using \code{\link{invoke}}
}
\description{
Creates a pipeline out of a set of pipes
}
