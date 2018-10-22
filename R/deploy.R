sed_inplace <- function(previous, new, file) {
    previous <- gsub(pattern = '"', replacement = '\\"', fixed = T, x = previous)
    new <- gsub(pattern = '"', replacement = '\\"', fixed = T, x = new)
    return(paste0("sed -i \"s/", previous, "/", new, "/g\" ", file))
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
    all_images <- system2(command = "docker", "images", stdout = T)
    image_exists <- any(grepl(pattern = image_name, x = all_images, fixed = T))
    if(image_exists) {
        system2(command = "docker", args = paste("rmi", image_name))
    }
}

#' Builds a package from your pipeline
#'
#' @param model_function The model function from \code{find_best_models}
#' @param package_name The name of the package
#' @param libraries A list of library names required by the package. Defaults to all loaded non-base packages
#' @param tar_file The name and path of the final tarred package
#' @param prediction_precision Number of digits to be returned by the prediction
#' @param may_overwrite_tar_file Flag indicating if, when \code{tar_file} exists, this function is allowed to override it.
#'
#' @details A tip for finding out which libraries you need: start with just datapiper, build the package and check the output of building the package.
#' It will display which functions you use that aren't included yet.
#'
#' @return A logical: TRUE for success and FALSE for failure
#' @importFrom devtools document build create
#' @export
build_model_package <- function(model_function, package_name = "deploymodel", libraries = utils::sessionInfo()$otherPkgs, tar_file = "deploy.tar.gz",
                                prediction_precision = 8, may_overwrite_tar_file = F) {
    stopifnot(!missing(model_function), is.character(libraries))
    is_valid_package_name <- grepl(pattern = "^[a-zA-Z0-9\\.]+$", libraries)

    if(any(!is_valid_package_name)) warning(paste("Possible typo in package name:", paste0(collapse = ", ", libraries[!is_valid_package_name])))

    stopifnot(
        is.logical(may_overwrite_tar_file),
        may_overwrite_tar_file || !file.exists(tar_file),
        is.function(model_function),
        is.character(libraries),
        is.numeric(prediction_precision), prediction_precision >= 0
    )

    current_ws <- getwd()
    has_succeeded <- F

    tryCatch({
        # Setup temp dir with package
        tmpdir <- tempdir()
        package_path <- paste0(tmpdir, "/", package_name)
        if(dir.exists(package_path)) unlink(package_path, recursive = T)
        devtools::create(path = package_path)
        setwd(package_path)

        # Always add jsonlite since we need it for exporting / importing data
        if(!"jsonlite" %in% libraries) libraries <- c(libraries, "jsonlite")
        if(!package_name %in% libraries) libraries <- c(libraries, package_name)

        dir.create("data")
        save(model_function, file = "data/model.rda")
        save(libraries, file = "data/libraries.rda")

        target_script_file <- file(description = "file://R/deploy_model.R", open = "w")
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
#\' @return Predictions
#\' @export

predict_model <- function(input){
    for(lib in libraries) {
        library(lib, character.only = T)
    }

    if(is.data.frame(input)) {
        input_df <- input
    } else {
        input_df <- jsonlite::fromJSON(txt = input, flatten = T)
    }
    predictions <- model_function(input_df)

    # res <- jsonlite::toJSON(x = predictions, dataframe = "rows",
    #                         Date = "ISO8601", POSIXt = "ISO8601", factor = "string", complex = "list", raw = "base64",
    #                         null = "null", na = "null", digits = ', prediction_precision, ', pretty = F)
    # return(res)
    return(predictions)
}
#-------------------#
# Script stops here #
#-------------------#
')
        cat(script, file = target_script_file, append = F)
        close(target_script_file)

        devtools::document()
        build_package <- devtools::build()

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
#' @param model_library_file A model package created using \code{\link{build_model_package}}
#' @param package_name The name of the package
#' @param libraries A list of library names required by the package. Defaults to all loaded non-base packages.
#' Has support for github (\code{\link[devtools]{install_github}}) and CRAN packages.
#' Any library with a \code{/} in it will be assumed to be a github package, others will be assumed to be CRAN packages.
#' The datapiper package will be automatically substituted by the github version.
#' @param docker_image_name The name of the docker image
#' @param additional_build_commands Additional build command that need to be executed. Will be executed after all other commands have run. Character vector.
#' @param may_overwrite_docker_image Flag indicating if, when \code{model_library_file} exists, this function is allowed to override it.
#'
#' @details Note: by default ports 80 and 8004 are exposed on the image. This function does not change anything about that. Furthermore, this
#' function does not handle security for you. That is your responsibility.
#' By default we use the opencpu/base image. See opencpu.org for details on configuration.
#'
#' @return A logical: TRUE for success and FALSE for failure
#' @export
build_docker <- function(model_library_file, package_name = "deploymodel", libraries = utils::sessionInfo()$otherPkgs, docker_image_name = "model_image",
                         additional_build_commands = "", may_overwrite_docker_image = F){

    # TODO
    # ALLOW FOR CHOOSING R VERSION
    # REMOVE MANUAL INSTALLATION OF datapiper
    # ENABLE AUTOMATIC CORRECTION OF datapiper LOCATION
    is_valid_library_name <- grepl(pattern = "^[a-zA-Z0-9\\./]+$", libraries)
    stopifnot(
        is_docker_running(),
        may_overwrite_docker_image || !file.exists(docker_image_name),
        !any(!is_valid_library_name),
        is.character(additional_build_commands)
    )

    if(!"jsonlite" %in% libraries) libraries <- c(libraries, "jsonlite")
    if("datapiper" %in% libraries) libraries[libraries == "datapiper"] <- "jeroenvdhoven/datapiper"
    else if(!"jeroenvdhoven/datapiper" %in% libraries) libraries <- c(libraries, "jeroenvdhoven/datapiper")


    # Prep directory for building
    daemon_name = "opencpu/base"
    build_dir <- tempdir()
    file.copy(model_library_file, build_dir)
    file.copy("../datapiper_0.1.2.999.tar.gz", build_dir)

    # Determine where we need to pull each library from
    is_cran_lib <- grepl(pattern = "^[a-zA-Z0-9\\.]+$", x = libraries)
    is_github_lib <- grepl(pattern = "/", fixed = T, x = libraries)
    cran_libs <- libraries[is_cran_lib]
    github_libs <- libraries[is_github_lib]

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
                                     "RUN R -e 'install.packages(\"devtools\")' \n",
                                     "RUN R -e 'devtools::install_github(", to_string_array(github_libs), ")'"
                                 ))
        add_preloaded_library_command <- sed_inplace(
            previous = '"preload": \\["lattice"\\]',
            new = paste0('"preload": \\["lattice", "', package_name, '"\\]'),
            file = "'/etc/opencpu/server.conf'")

        # Create dockerfile
        dockerfile_content <- paste0("
FROM ", daemon_name, "
# Install dependency R packages
", cran_command, "
", github_command, "

# Install model package
COPY ", model_library_file, " /", model_library_file, "
RUN R -e 'install.packages(pkgs = \"/", model_library_file, "\", repos = NULL, type = \"source\")'

# Preloaded library command
RUN ", add_preloaded_library_command, "
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
#' @param image_name The name of the docker image
#' @param package_name The name of the package
#' @param process_name The name you want the docker image to be run as. Defaults to \code{image_name}
#' @param batch_size Allows you to set how many rows you want to send each time
#' @param ping_time How many seconds we'll try to ping the docker image before declaring the launch a failure.
#' @param verbose Flag indicating if you want status updates printed
#'
#' @return A dataframe of predictions, one row per row in \code{data}
#' @export
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom httr POST GET
test_docker <- function(data, image_name, process_name = image_name, package_name = "deploymodel", batch_size = nrow(data), ping_time = 5, verbose = T) {
    stopifnot(
        is.data.frame(data),
        is.character(image_name),
        length(image_name) == 1,
        is_docker_running()
    )

    if(verbose) cat("Starting image\n")
    system2(command = "docker", args = paste("run --name", process_name, "-t -p 8004:8004", image_name), wait = F, stdout = F)

    if(verbose) cat("Start pinging for server to be up\n")
    current_time <- Sys.time()
    test_time <- Sys.time()
    while(difftime(time1 = test_time, time2 = current_time, units = "s") < ping_time){
        tryCatch({
            test_time <- Sys.time()
            response <- httr::GET("http://localhost:8004/ocpu/library/deploymodel/R/predict_model/json")
            break
        }, error = function(e) {
            test_time <- Sys.time()
        })
    }
    if(difftime(time1 = test_time, time2 = current_time, units = "s") > ping_time) stop("Error: server not up after ", ping_time, " seconds")

    if(verbose) cat("Start predictions\n")
    result <- data.frame()
    batch_start_indices <- seq.int(from = 1, to = nrow(data), by = batch_size)
    model_url <- paste0("http://localhost:8004/ocpu/library/", package_name, "/R/predict_model/json")
    for(batch_start in batch_start_indices){
        tryCatch({
            batch_end <- min(nrow(data), batch_start + batch_size - 1)
            cat("Sending rows", batch_start, "to", batch_end, "out of", nrow(data), "\n")
            subset <- data[batch_start:batch_end,]

            data_json <- jsonlite::toJSON(
                x = subset, dataframe = "rows", Date = "ISO8601", POSIXt = "ISO8601",
                factor = "string", complex = "list", raw = "base64", null = "null",
                na = "null", digits = 8, pretty = F)

            response <- httr::POST(url = model_url,
                                   encode = "json", body = list("input" = data_json))
            predictions <- jsonlite::fromJSON(flatten = T, txt = rawToChar(response$content))

            if(nrow(result) == 0) result <- predictions
            else result <- dplyr::bind_rows(result, predictions)
        }, error = function(e){
            kill_docker_process(process_name = process_name)
            stop(paste(
                "Error executing batch", batch_start, "to", batch_end, ":\n",
                e
            ))
        })
    }
    if(verbose) cat("Finished predictions\n")
    if(nrow(result) != nrow(data)) warning(paste("Error: dataset had", nrow(data), "rows, predictions gave", nrow(result), "predictions"))

    if(verbose) cat("Stopping image\n")
    kill_docker_process(process_name = process_name)

    return(result)
}

# train <- readr::read_csv("train.csv")
# load("tmp.Rdata")

# package_n <- "the.model"
# lib_file <- paste0(package_n, ".tar.gz")
# image_name <- paste0(package_n, ".image")
# build_model_package(model_function = best_models$.predict, tar_file = lib_file, libraries = c("dplyr", "xgboost", "magrittr", "datapiper"), may_overwrite_tar_file = T, package_name = package_n)
# build_docker(model_library_file = lib_file, docker_image_name = image_name, libraries = c("dplyr", "xgboost"), package_name = package_n)
# res <- test_docker(data = train, image_name = image_name, process_name = image_name, package_name = package_n)

# FINAL_TEST <- readr::read_csv("test.csv")
# res <- test_docker(data = FINAL_TEST, image_name = image_name, process_name = image_name, package_name = package_n)

# install.packages("deploy.tar.gz", type = "source", repos = NULL)
# tst_json <- jsonlite::toJSON(x = train, dataframe = "rows", Date = "ISO8601", POSIXt = "ISO8601",
#   factor = "string", complex = "list", raw = "base64", null = "null", na = "null", digits = 8, pretty = F)
# the.model::predict_model(tst_json)

# docker run --name deployed_model -t -p 8004:8004 the.model &
# docker stop deployed_model; docker rm deployed_model
# curl http://localhost:8004/ocpu/library/the.model/R/predict_model/json  -H "Content-Type: application/json"  -d '{"input" : "[ {\"Id\":1,\"MSSubClass\":60,\"MSZoning\":\"RL\",\"LotFrontage\":65,\"LotArea\":8450,\"Street\":\"Pave\",\"Alley\":null,\"LotShape\":\"Reg\",\"LandContour\":\"Lvl\",\"Utilities\":\"AllPub\",\"LotConfig\":\"Inside\",\"LandSlope\":\"Gtl\",\"Neighborhood\":\"CollgCr\",\"Condition1\":\"Norm\",\"Condition2\":\"Norm\",\"BldgType\":\"1Fam\",\"HouseStyle\":\"2Story\",\"OverallQual\":7,\"OverallCond\":5,\"YearBuilt\":2003,\"YearRemodAdd\":2003,\"RoofStyle\":\"Gable\",\"RoofMatl\":\"CompShg\",\"Exterior1st\":\"VinylSd\",\"Exterior2nd\":\"VinylSd\",\"MasVnrType\":\"BrkFace\",\"MasVnrArea\":196,\"ExterQual\":\"Gd\",\"ExterCond\":\"TA\",\"Foundation\":\"PConc\",\"BsmtQual\":\"Gd\",\"BsmtCond\":\"TA\",\"BsmtExposure\":\"No\",\"BsmtFinType1\":\"GLQ\",\"BsmtFinSF1\":706,\"BsmtFinType2\":\"Unf\",\"BsmtFinSF2\":0,\"BsmtUnfSF\":150,\"TotalBsmtSF\":856,\"Heating\":\"GasA\",\"HeatingQC\":\"Ex\",\"CentralAir\":\"Y\",\"Electrical\":\"SBrkr\",\"1stFlrSF\":856,\"2ndFlrSF\":854,\"LowQualFinSF\":0,\"GrLivArea\":1710,\"BsmtFullBath\":1,\"BsmtHalfBath\":0,\"FullBath\":2,\"HalfBath\":1,\"BedroomAbvGr\":3,\"KitchenAbvGr\":1,\"KitchenQual\":\"Gd\",\"TotRmsAbvGrd\":8,\"Functional\":\"Typ\",\"Fireplaces\":0,\"FireplaceQu\":null,\"GarageType\":\"Attchd\",\"GarageYrBlt\":2003,\"GarageFinish\":\"RFn\",\"GarageCars\":2,\"GarageArea\":548,\"GarageQual\":\"TA\",\"GarageCond\":\"TA\",\"PavedDrive\":\"Y\",\"WoodDeckSF\":0,\"OpenPorchSF\":61,\"EnclosedPorch\":0,\"3SsnPorch\":0,\"ScreenPorch\":0,\"PoolArea\":0,\"PoolQC\":null,\"Fence\":null,\"MiscFeature\":null,\"MiscVal\":0,\"MoSold\":2,\"YrSold\":2008,\"SaleType\":\"WD\",\"SaleCondition\":\"Normal\",\"SalePrice\":208500} ]"}'
# docker rmi $(docker images -f "dangling=true" -q) -f

# for build_docker
# RUN R -e 'install.packages(pkgs = c(\"purrr\", \"dplyr\", \"data.table\", \"xgboost\"))'
#
# # Install datapiper
# COPY datapiper_0.1.2.999.tar.gz /datapiper
# RUN R -e 'install.packages(pkgs = \"/datapiper\", repos = NULL, type = \"source\")'
