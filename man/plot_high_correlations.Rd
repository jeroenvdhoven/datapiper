% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/correlation_removal.R
\name{plot_high_correlations}
\alias{plot_high_correlations}
\title{Plots highly correlated features as a graph}
\usage{
plot_high_correlations(high_cors, ...)
}
\arguments{
\item{high_cors}{Result of \code{\link[datapiper]{high_correlation_features}}. Can also provide your own graph generated using \code{\link[igraph]{graph.data.frame}}.
This is especially useful when combined with \code{\link[igraph]{decompose.graph}} to view subsections of the correlation graph.}

\item{...}{arguments to be passed to plot.igraph. \code{vertex.size} and \code{vertex.label.cex} (for label size) are probably good parameters to pass.}
}
\description{
Plots highly correlated features as a graph
}
