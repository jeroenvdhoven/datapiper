% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/feature_generation.R
\name{feature_interactions_predict}
\alias{feature_interactions_predict}
\title{Computes interaction effects for a new dataset}
\usage{
feature_interactions_predict(data, columns, column_means, max_interactions)
}
\arguments{
\item{data}{The new dataset, as a data.frame or data.table.}

\item{columns}{Column names from the \code{\link{pipe_feature_interactions}}'s results}

\item{column_means}{Column means from the \code{\link{pipe_feature_interactions}}'s results}

\item{max_interactions}{The maximum number of columns that will be considered for interaction effects per variable. Should be the same as the one used for
the results of \code{\link{pipe_feature_interactions}}.}
}
\value{
The data frame in \code{data} with the interaction effects
}
\description{
Computes interaction effects for a new dataset
}
