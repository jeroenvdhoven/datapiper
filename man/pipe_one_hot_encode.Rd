% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/transform_features.R
\name{pipe_one_hot_encode}
\alias{pipe_one_hot_encode}
\title{Train one-hot encoding}
\usage{
pipe_one_hot_encode(train,
  columns = colnames(train)[purrr::map_lgl(train, function(x)
  return(!(is.numeric(x) || is.logical(x))))], stat_functions, response,
  quantile_trim_threshold = 0, use_pca = FALSE, pca_tol = 0.1)
}
\arguments{
\item{train}{The train dataset, as a data.frame or data.table. Data.tables may be changed by reference.}

\item{columns}{Columns from \code{train} to use for one-hot-encoding. Will automatically check if theses are column names in \code{train}}

\item{stat_functions}{A (named) list of functions for when you want to use mean-encoding. Don't set it if you want to do regular one-hot encoding.
Any function that return a single value from a scalar would do (e.g. quantile, sd).}

\item{response}{String denoting the name of the column that should be used as the response variable. Mandatory}

\item{quantile_trim_threshold}{Sets \code{quantile_trim_threshold} for \code{\link{pipe_create_stats}} if you provided \code{stat_functions}}

\item{use_pca}{Whether PCA transformation is required.}

\item{pca_tol}{The \code{tol} of \code{\link[stats]{prcomp}}}
}
\value{
A list containing the transformed train dataset and a trained pipe.
}
\description{
Train one-hot encoding
}
