sed_inplace <- function(previous, new, file) {
    previous <- gsub(pattern = '"', replacement = '\\"', fixed = T, x = previous)
    new <- gsub(pattern = '"', replacement = '\\"', fixed = T, x = new)
    return(paste0("sed -i \"\" ", file, " \"s/", previous, "/", new, "/g\" "))
}
to_string_array <- function(s) paste0("c(", paste0('"', s, '"', collapse = ", "), ")")

is_docker_running <- function() {
    # Check if docker is installed
    docker_path <- Sys.which("docker")
    if(docker_path == "") stop("Error: docker not found. Install docker or add it to your PATH")

    # Check if we can call docker images
    errors <- system2(command = "docker", args = "images", stdout = F, stderr = "")
    return(errors == 0)
}

kill_docker_process <- function(process_name) {
    tryCatch({
        system2(command = "docker", args = paste("stop", process_name))
        system2(command = "docker", args = paste("rm", process_name))
    }, error = function(e){
        stop(paste("Error while trying to stop docker image:\n", e))
    })
}

delete_image <- function(image_name) {
    stopifnot(is_docker_running())
    all_images <- system2(command = "docker", "images", stdout = T)
    image_exists <- any(grepl(pattern = image_name, x = all_images, fixed = T))
    if(image_exists) {
        system2(command = "docker", args = paste("rmi", image_name))
    }
}

#' Builds a package from your pipeline
#'
#' @param trained_pipeline The model function from \code{find_best_models}
#' @param package_name The name of the package
#' @param libraries A list of library names required by the package. Defaults to all loaded non-base packages
#' @param tar_file The name and path of the final tarred package
#' @param extra_variables A character vector of variables names to be included in the package. Used to include extra fuctions and variables in the packages
#'  that are not contained in any of the packages listed in \code{libraries}.
#' @param may_overwrite_tar_file Flag indicating if, when \code{tar_file} exists, this function is allowed to override it.
#' @param verbose Flag indicating if devtools functions should print anything.
#'
#' @details A tip for finding out which libraries you need: start with just datapiper, build the package and check the output of building the package.
#' It will display which functions you use that aren't included yet.
#'
#' @return A logical: TRUE for success and FALSE for failure
#' @importFrom usethis create_package
#' @importFrom devtools document build
#' @export
build_model_package <- function(trained_pipeline, package_name = "deploymodel", libraries = names(utils::sessionInfo()$otherPkgs),
                                tar_file = "deploy.tar.gz", extra_variables = character(0),
                                may_overwrite_tar_file = F, verbose = F) {
    stopifnot(
        !missing(trained_pipeline),
        is.character(libraries),
        is.character(extra_variables),
        is.logical(verbose)
    )
    is_valid_package_name <- grepl(pattern = "^[a-zA-Z0-9\\.]+$", libraries)

    if(any(!is_valid_package_name)) warning(paste("Possible typo in package name:", paste0(collapse = ", ", libraries[!is_valid_package_name])))

    stopifnot(
        is.logical(may_overwrite_tar_file),
        may_overwrite_tar_file || !file.exists(tar_file),
        is.pipe(trained_pipeline) || is.pipeline(trained_pipeline)
    )

    if(is.pipeline(trained_pipeline)) trained_pipeline <- flatten_pipeline(trained_pipeline)

    current_ws <- getwd()
    on.exit(setwd(current_ws))
    has_succeeded <- F

    # If any extra functions are provided, ensure they will be added correctly.
    for(func in extra_variables) get(x = func)

    tryCatch({
        # Setup temp dir with package
        tmpdir <- tempdir()
        package_path <- paste0(tmpdir, "/", package_name)
        if(dir.exists(package_path)) unlink(package_path, recursive = T)

        # Always add jsonlite and plumber since we need it for exporting / importing data
        if(!"jsonlite" %in% libraries) libraries <- c(libraries, "jsonlite")
        if(!"plumber" %in% libraries) libraries <- c(libraries, "plumber")

        usethis::create_package(path = package_path, rstudio = F, fields = list(
            # "Imports" = paste0(libraries, collapse = " "),
            "Version" = "1.0.0",
            "Date" = Sys.Date(),
            "Description" = "Data pipeline deployed automatically with datapiper"
        ), open = F)
        dir.create(file.path(package_path, "data"), showWarnings = FALSE)
        setwd(package_path)


        save(trained_pipeline, file = "data/model.rda")
        save(libraries, file = "data/libraries.rda")
        if(length(extra_variables) > 0)
            save(list = extra_variables, file = "data/extra_variables.rda")

        save(invoke, file = "data/invoke.rda")
        save(invoke.pipe, file = "data/invoke.pipe.rda")
        save(invoke.pipeline, file = "data/invoke.pipeline.rda")
        save(select_cols, file = "data/select_cols.rda")
        save(deselect_cols, file = "data/deselect_cols.rda")

        target_script_file <- file(description = "file://R/predictions.R", open = "w")
        script <- paste0(
            '
# Auto generated script: please do not edit.
#--------------------#
# Script starts here #
#--------------------#
.onLoad <- function(lib, pkg){
#automatically loads the dataset when package is loaded
#do not use this in combination with lazydata=true
utils::data(libraries, package = pkg, envir = parent.env(environment()))
utils::data(model, package = pkg, envir = parent.env(environment()))
}

#\' Call predict on the provided model and the new input
#\'
#\' @param input Either a dataframe or a JSON string.
#\'
#\' @return Predictions from the pipeline
#\' @export
', paste0("#\' @import ", libraries, collapse = "\n"), '
predict_model <- function(input){
    for(lib in libraries) {
        library(lib, character.only = T)
    }

    if(is.data.frame(input)) {
        input_df <- input
    } else {
        input_df <- jsonlite::fromJSON(txt = input, flatten = T)
    }
    predictions <- invoke(trained_pipeline, input_df)
    return(predictions)
}
#-------------------#
# Script stops here #
#-------------------#

#------------------#
# Plumber function #
#------------------#
#\' Turns the predict_model function into a plumber endpoint
#\'
#\' @param current_plumber_env NULL or a previously initialised plumber object, using \\code{plumber::plumber$new()}
#\'
#\' @return \\code{current_plumber_env} with an endpoint added for this package\'s predict_model function. It can be found on
#\' <package_name>/predict_model
#\' @export
#\' @importFrom plumber plumber
create_plumber_endpoint <- function(current_plumber_env = NULL) {
    is_plumber <- function(x) "plumber" %in% class(x)

    if(is.null(current_plumber_env)) {
        current_plumber_env <- plumber::plumber$new()
    } else stopifnot(is_plumber(current_plumber_env))

    current_plumber_env$handle(methods = "GET", path = "/', package_name, '/predict_model", handler = predict_model)
    return(current_plumber_env)
}
')
        cat(script, file = target_script_file, append = F)
        close(target_script_file)

        devtools::document()
        build_package <- devtools::build(quiet = !verbose)

        result_file <- paste0(current_ws, "/", tar_file)
        if(file.exists(result_file)) file.remove(result_file)
        file.rename(from = build_package, to = result_file)

        setwd("..")
        unlink(package_name, recursive = T)
        setwd(current_ws)

        has_succeeded <- T
    }, error = function(e) {
        e <- paste0("Encountered error:\n", as.character(e))
        warning(e)
        setwd(current_ws)
    })
    return(has_succeeded)
}


#' Build a docker image out of a model function
#'
#' @param model_library_file A vector of paths to model packages created using \code{\link{build_model_package}}.
#' @param package_name A vector containing the names of the packages to be installed. Should be the same length as \code{model_library_file}.
#' @param libraries A list of library names required by the package. Defaults to all loaded non-base packages.
#' Has support for github (\code{\link[devtools]{install_github}}) and CRAN packages.
#' Any library with a \code{/} in it will be assumed to be a github package, others will be assumed to be CRAN packages.
#' The datapiper package will be automatically substituted by the github version if presented, though you can omit this package. Do make sure you use the dependencies
#' for the pipe functions you use. See \code{\link[datapiper]{pipe}} for details if you encounter problems with for instance missing functions.
#'
#' It is strongly recommended to use the same libraries here as used in \code{\link{build_model_package}}, since \code{\link{build_model_package}} will try loading those libraries.
#' @param docker_image_name The name of the docker image
#' @param additional_build_commands Additional build command that need to be executed. Will be executed after all other commands have run. Character vector.
#' @param may_overwrite_docker_image Flag indicating if, when \code{model_library_file} exists, this function is allowed to override it.
#' @param docker_image_type The type of docker image to be created. Currently `opencpu` and `plumber` are supported.
#' Both will run by default on port 8004 of the created image.
#'
#' @details Note: by default ports 80 (OpenCPU only) and 8004 are exposed on the image. This function does not change anything about that. Furthermore, this
#' function does not handle security for you. That is your responsibility.
#' By default we use the opencpu/base image for OpenCPU and r-base for plumber. See their respective websites for further details.
#'
#' @return A logical: TRUE for success and FALSE for failure
#' @export
build_docker <- function(model_library_file, package_name = "deploymodel", libraries = names(utils::sessionInfo()$otherPkgs),
                         docker_image_name = "model_image",
                         docker_image_type = c("opencpu", "plumber")[1],
                         additional_build_commands = "", may_overwrite_docker_image = F){

    # TODO
    # ALLOW FOR CHOOSING R VERSION
    is_valid_library_name <- grepl(pattern = "^[a-zA-Z0-9\\./]+$", libraries)
    stopifnot(
        is_docker_running(),
        may_overwrite_docker_image || !file.exists(docker_image_name),
        !any(!is_valid_library_name),
        is.character(model_library_file), !any(!file.exists(model_library_file)),
        is.character(package_name), length(package_name) == length(model_library_file),
        length(docker_image_type) == 1, docker_image_type %in% c("plumber", "opencpu"),
        is.character(additional_build_commands)
    )


    # Datapiper will be installed separately at the beginning to improve the installation process.
    if(any(libraries == "datapiper")) libraries[libraries == "datapiper"] <- "jeroenvdhoven/datapiper"
    if(!"jsonlite" %in% libraries) libraries <- c(libraries, "jsonlite")
    if(!"plumber" %in% libraries) libraries <- c(libraries, "plumber")

    # Prep directory for building
    if(docker_image_type == "opencpu") {
        daemon_name = "opencpu/base"
        pre_installation_command <- ""

        # preload_libraries <- paste("RUN", sed_inplace(
        #     previous = '"preload": ["lattice"]',
        #     new = paste0('"preload": ["lattice", "', package_name, '"]'),
        #     file = "'/etc/opencpu/server.conf'"))

        # additional_build_commands <- c(additional_build_commands, preload_libraries)
    } else if(docker_image_type == "plumber") {
        daemon_name = "r-base"

        prepare_plumber_endpoints <- paste0('pr <- ', package_name[1], '::create_plumber_endpoint()')
        if(length(package_name) > 1) {
            generate_other_endpoints <- paste0(
                collapse = ";", 'pr <- ', package_name[-1], '::create_plumber_endpoint(pr)'
            )
            prepare_plumber_endpoints <- paste(sep = ";", prepare_plumber_endpoints, generate_other_endpoints)
        }

        pre_installation_command <- "RUN apt-get update && apt-get -y install libcurl4-gnutls-dev libssl-dev --no-install-recommends && rm -rf /var/lib/apt/lists/*"
        run_plumber_command <- c(
            'EXPOSE 8004',
            paste0('CMD ["R", "-e", "', prepare_plumber_endpoints, '; pr$run(host=\'0.0.0.0\', port=8004)"]')
        )

        additional_build_commands <- c(additional_build_commands, run_plumber_command)
    }

    build_dir <- tempdir()
    file.copy(model_library_file, build_dir)

    # Determine where we need to pull each library from
    is_cran_lib <- grepl(pattern = "^[a-zA-Z0-9\\.]+$", x = libraries)
    is_github_lib <- grepl(pattern = "/", fixed = T, x = libraries)

    cran_libs <- libraries[is_cran_lib]
    github_libs <- libraries[is_github_lib]

    # Make sure we install devtools if we require github libs
    if(any(is_github_lib) && !"devtools" %in% cran_libs) cran_libs <- c("devtools", cran_libs)

    # Concat all additional build commands, separated by \n
    additional_build_commands <- paste0(collapse = "\n", additional_build_commands)

    success <- F
    tryCatch({
        # Pull in base image
        system2(command = "docker", args = paste("pull", daemon_name), stdout = "", stderr = "")

        # Create command for downloading packages
        cran_command <- ifelse(length(cran_libs) > 0, no = "# No cran command needed",
                               yes = paste0("RUN R -e 'install.packages(", to_string_array(cran_libs), ")'"))
        github_command <- ifelse(length(github_libs) > 0, no = "# No github command needed",
                                 yes = paste0(
                                     "RUN R -e 'devtools::install_github(", to_string_array(github_libs), ")'"
                                 ))
        # Create dockerfile
        dockerfile_content <- paste0("
FROM ", daemon_name, "
# Make sure we can install the devtools package
", pre_installation_command, "

# Install other required R packages
", cran_command, "
", github_command, "

# Install model package
COPY ", paste(model_library_file, collapse = " "), " /datapiper_raw_packages/
RUN R -e 'install.packages(pkgs = paste0(\"datapiper_raw_packages/\", list.files(\"datapiper_raw_packages\")), repos = NULL, type = \"source\")'

# Preloaded library command
", additional_build_commands)

        # cat(dockerfile_content)
        dockerfile_path <- paste0(build_dir, "/dockerfile")
        file.create(dockerfile_path)
        cat(file = dockerfile_path, dockerfile_content)

        # Build docker image.
        system2(command = "docker", args = paste("build -f", dockerfile_path, build_dir, "-t", docker_image_name))

        success <- T
    }, error = function(e){
        e <- paste0("Encountered error:\n", as.character(e))
        warning(e)
    })
    return(success)
}

#' Test your docker image
#'
#' @param data A new dataset to get predictions for
#' @param package_name The name of the package
#' @param image_name The name of the docker image
#' @param process_name The name you want the docker image to be run as. Defaults to \code{image_name}
#' @param base_url The base url where the docker image is located. If this is equals \code{"localhost"}, this function will also start and stop the image.
#' @param port The host port on which the docker image is accessible. Defaults to 8004.
#' @param batch_size Allows you to set how many rows you want to send each time.
#' @param ping_time How many seconds we'll try to ping the docker image before declaring the launch a failure.
#' @param verbose Flag indicating if you want status updates printed.
#' @param docker_image_type The type of docker image to be created. Currently `opencpu` and `plumber` are supported.
#' Both will run by default on port 8004 of the created image.
#'
#' @return A dataframe of predictions, one row per row in \code{data}
#' @export
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom httr POST GET
test_docker <- function(data, package_name, image_name = package_name, process_name = image_name,
                        base_url = "localhost", port = 8004,
                        docker_image_type = c("opencpu", "plumber")[1],
                        batch_size = nrow(data), ping_time = 5, verbose = T) {
    stopifnot(
        is.data.frame(data),
        is.character(image_name),
        length(image_name) == 1,
        length(package_name) == 1,
        length(docker_image_type) == 1, docker_image_type %in% c("plumber", "opencpu"),
        length(process_name) == 1
    )

    if(docker_image_type == "opencpu") {
        model_url <- paste0("http://", base_url, ":", port, "/ocpu/library/", package_name, "/R/predict_model/json")
    } else if(docker_image_type == "plumber"){
        model_url <- paste0("http://", base_url, ":", port, "/", package_name, "/predict_model")
    }
    if(verbose) cat("Url:", model_url, "\n")

    if(base_url == "localhost"){
        stopifnot(is_docker_running())
        if(verbose) cat("Starting image\n")
        system2(command = "docker", args = paste0("run --name ", process_name, " -t -p ", port, ":", port, " ", image_name), wait = F, stdout = F)
    }

    if(verbose) cat("Start pinging for server to be up\n")
    current_time <- Sys.time()
    test_time <- Sys.time()
    while(difftime(time1 = test_time, time2 = current_time, units = "s") < ping_time){
        tryCatch({
            test_time <- Sys.time()
            response <- httr::GET(model_url)
            break
        }, error = function(e) {
            test_time <- Sys.time()
        })
    }
    if(difftime(time1 = test_time, time2 = current_time, units = "s") > ping_time) stop("Error: server not up after ", ping_time, " seconds")

    if(verbose) cat("Start predictions\n")
    result <- data.frame()
    batch_start_indices <- seq.int(from = 1, to = nrow(data), by = batch_size)

    for(batch_start in batch_start_indices){
        tryCatch({
            batch_end <- min(nrow(data), batch_start + batch_size - 1)
            cat("Sending rows", batch_start, "to", batch_end, "out of", nrow(data), "\n")
            subset <- data[batch_start:batch_end,]

            data_json <- jsonlite::toJSON(
                x = subset, dataframe = "rows", Date = "ISO8601", POSIXt = "ISO8601",
                factor = "string", complex = "list", raw = "base64", null = "null",
                na = "null", digits = 8, pretty = F)

            if(docker_image_type == "opencpu") {
                response <- httr::POST(url = model_url, encode = "json", body = list("input" = data_json))
            } else if(docker_image_type == "plumber") {
                response <- httr::GET(url = model_url, query = list("input" = data_json))
            }
            predictions <- jsonlite::fromJSON(flatten = T, txt = rawToChar(response$content))

            if(nrow(result) == 0) result <- predictions
            else result <- dplyr::bind_rows(result, predictions)
        }, error = function(e){
            kill_docker_process(process_name = process_name)
            stop(paste(
                "Error executing batch", batch_start, "to", batch_end, "\n",
                "Response from image:", rawToChar(response$content), "\n",
                "Error: \n",
                e
            ))
        })
    }
    if(verbose) cat("Successfully finished predictions\n")
    if(!is.data.frame(result)) warning(paste("Warning: 0 predictions were returned"))
    else if(nrow(result) != nrow(data)) warning(paste("Warning: dataset had", nrow(data), "rows, predictions gave", nrow(result), "predictions"))

    if(base_url == "localhost"){
        if(verbose) cat("Stopping image\n")
        kill_docker_process(process_name = process_name)
    }

    return(result)
}
