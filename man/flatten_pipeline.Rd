% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pipeline.R
\name{flatten_pipeline}
\alias{flatten_pipeline}
\title{Flattens a pipeline so it does not contain any more sub-pipelines, only pipe elements.}
\usage{
flatten_pipeline(p)
}
\arguments{
\item{p}{A pipeline}
}
\value{
The pipeline \code{p}, but without sub-pipelines
}
\description{
Flattens a pipeline so it does not contain any more sub-pipelines, only pipe elements.
}
