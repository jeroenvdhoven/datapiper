% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/transform_features.R
\name{pipe_scaler}
\alias{pipe_scaler}
\title{Rescales data to standardised ranges}
\usage{
pipe_scaler(train, exclude_columns = character(length = 0),
  type = "[0-1]", retransform_columns)
}
\arguments{
\item{train}{The train dataset, as a data.frame or data.table. Data.tables may be changed by reference.}

\item{exclude_columns}{Names of columns that should be excluded from rescaling}

\item{type}{Type of rescales to perform:
\itemize{
\item \code{"[0-1]"}: rescales the columns to the [0-1] range
\item \code{"N(0,1)"}: rescales the columns to mean 0 and sd 1
}}

\item{retransform_columns}{Columns that should be rescaled later on. A new pipe will be created and returned as a separate list entry.}
}
\value{
A list containing the transformed train dataset and a trained pipe. If \code{retransform_columns} was set, the reverting pipe will also be provided.
}
\description{
Rescales data to standardised ranges
}
