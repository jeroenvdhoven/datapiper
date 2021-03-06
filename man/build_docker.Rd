% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/deploy.R
\name{build_docker}
\alias{build_docker}
\title{Build a docker image out of a model function}
\usage{
build_docker(model_library_file, package_name = "deploymodel",
  libraries = names(utils::sessionInfo()$otherPkgs),
  docker_image_name = "model_image", docker_image_type = c("opencpu",
  "plumber")[1], additional_build_commands = "",
  may_overwrite_docker_image = F)
}
\arguments{
\item{model_library_file}{A vector of paths to model packages created using \code{\link{build_model_package}}.}

\item{package_name}{A vector containing the names of the packages to be installed. Should be the same length as \code{model_library_file}.}

\item{libraries}{A list of library names required by the package. Defaults to all loaded non-base packages.
Has support for github (\code{\link[devtools]{install_github}}) and CRAN packages.
Any library with a \code{/} in it will be assumed to be a github package, others will be assumed to be CRAN packages.
The datapiper package will be automatically substituted by the github version if presented, though you can omit this package. Do make sure you use the dependencies
for the pipe functions you use. See \code{\link[datapiper]{pipe}} for details if you encounter problems with for instance missing functions.

It is strongly recommended to use the same libraries here as used in \code{\link{build_model_package}}, since \code{\link{build_model_package}} will try loading those libraries.}

\item{docker_image_name}{The name of the docker image}

\item{docker_image_type}{The type of docker image to be created. Currently `opencpu` and `plumber` are supported.
Both will run by default on port 8004 of the created image.}

\item{additional_build_commands}{Additional build command that need to be executed. Will be executed after all other commands have run. Character vector.}

\item{may_overwrite_docker_image}{Flag indicating if, when \code{model_library_file} exists, this function is allowed to override it.}
}
\value{
A logical: TRUE for success and FALSE for failure
}
\description{
Build a docker image out of a model function
}
\details{
Note: by default ports 80 (OpenCPU only) and 8004 are exposed on the image. This function does not change anything about that. Furthermore, this
function does not handle security for you. That is your responsibility.
By default we use the opencpu/base image for OpenCPU and r-base for plumber. See their respective websites for further details.
}
