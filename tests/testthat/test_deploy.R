context("Deployment")
get_library_predictions <- function(library_name, test){
    library(library_name, character.only = T)
    result <- predict_model(test)
    # unloadNamespace(library_name)
    return(result)
}

generate_model_function <- function(extra_pipe, response = "x"){
    if(missing(extra_pipe)){
        trained_pipe <- datapiper::train_pipeline(
            datapiper::segment(.segment = datapiper::pipe_select, "x", "a", "b", "c", "s"),
            datapiper::segment(.segment = datapiper::pipe_scaler),
            datapiper::segment(.segment = datapiper::pipe_clustering, exclude_columns = response),
            response = response
        )
    } else {
        trained_pipe <- datapiper::train_pipeline(
            datapiper::segment(.segment = datapiper::pipe_select, "x", "a", "b", "c", "s"),
            datapiper::segment(.segment = datapiper::pipe_scaler),
            datapiper::segment(.segment = datapiper::pipe_clustering, exclude_columns = response),
            extra_pipe,
            response = response
        )
    }
    m <- datapiper::find_template_formula_and_data(response = response, training_function = lm)

    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]

    find_model_result <- datapiper::find_model(train = train, test = test, response = response, verbose = F,
                                               preprocess_pipes = list("one" = trained_pipe),
                                               models = list("lm" = m), metrics = list("rmse" = util_RMSE),
                                               parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)

    full_pipe <- datapiper::find_best_models(train = train, find_model_result = find_model_result,
                                             metric = "test_rmse", higher_is_better = F)

    return(list(
        train = train,
        test = test,
        full_pipe = full_pipe
    ))
}

describe("build_model_package()", {
    tar_file_name <- "tmp_test_package.tar.gz"
    library_name <- "test.package"
    libs <- c("dplyr", "magrittr")

    it("allows you to include extra variables", {
        # Assigment to global environment like this is unfortunately needed since running the tests automatically causes an error otherwise.
        sqrt_global_name <- "sqrt_substitute_function"
        .GlobalEnv[[sqrt_global_name]] <- sqrt
        sqrt_substitute_function <- get(sqrt_global_name)

        test_df <- data.frame(10)
        transformed_df <- sqrt_substitute_function(test_df)
        p <- pipeline(
            pipe(.function = function(data) {
                sqrt_substitute_function(data)
            })
        )

        result <- build_model_package(trained_pipeline = p,
                                      package_name = library_name,
                                      libraries = character(0),
                                      tar_file = tar_file_name,
                                      may_overwrite_tar_file = T,
                                      extra_variables = sqrt_global_name,
                                      verbose = F)
        expect_true(object = result, info = "Build function returned a success")
        expect_true(file.exists(tar_file_name))

        install.packages(tar_file_name, repos = NULL, verbose = F, quiet = T)
        rm(sqrt_substitute_function)
        r <- ctest_for_no_errors(test.package::predict_model(test_df), error_message = "Error: sqrt_substitute_function was not exported")
        expect_equal(r, transformed_df)

        remove.packages(pkgs = library_name)
        expect_true(file.remove(tar_file_name))
    })

    it("can build a package around a model pipeline", {
        r <- generate_model_function()
        train <- r$train
        test <- r$test
        full_pipe <- r$full_pipe

        ws_before_package <- getwd()
        result <- build_model_package(trained_pipeline = full_pipe,
                                      package_name = library_name,
                                      libraries = libs,
                                      tar_file = tar_file_name,
                                      may_overwrite_tar_file = T,
                                      verbose = F)
        expect_equal(ws_before_package, getwd(), info = "Workspace should not have changed")
        expect_true(object = result, info = "Build function returned a success")
        expect_true(file.exists(tar_file_name))

        install.packages(tar_file_name, repos = NULL, verbose = F, quiet = T)

        lib_predictions <- get_library_predictions(library_name = library_name, test = test)
        lib_df_predictions <- predict_model(test)
        function_predictions <- invoke(full_pipe, test)

        expect_equal(lib_predictions$one_lm_1, function_predictions$one_lm_1)
        expect_equal(lib_df_predictions$one_lm_1, function_predictions$one_lm_1)
        remove.packages(pkgs = library_name)

        it("also adds dependencies to NAMESPACE and DESCRIPTION exports", {
            extract_dir <- paste(Sys.time(), "tmp file for testing datapiper")
            if(file.exists(extract_dir)) stop("Error: expected output directory already exists for testing NAMESPACE and DESCRIPTION files")
            untar(tarfile = tar_file_name, exdir = extract_dir)

            namespace_file <- paste0(extract_dir, "/", library_name, "/NAMESPACE")
            description_file <- paste0(extract_dir, "/", library_name, "/DESCRIPTION")

            expect_true(file.exists(namespace_file))
            namespace_contents <- scan(file = namespace_file, what = "character", sep = "\n", quiet = T)
            for(lib in libs) expect_true(any(grepl(pattern = lib, x = namespace_contents)), info = paste(lib, "not found in namespace file"))

            expect_true(file.exists(description_file))
            description_contents <- scan(file = description_file, what = "character", sep = "\n", quiet = T)
            for(lib in libs) expect_true(any(grepl(pattern = lib, x = description_contents)), info = paste(lib, "not found in description file"))

            unlink(x = extract_dir, recursive = T)
        })

        expect_true(file.remove(tar_file_name))
    })

    it("can build a package around a model pipeline with a custom pipeline function", {
        custom_pipe <- function(train) {
            shuffle <- sample.int(n = nrow(train), size = nrow(train))
            train <- train[shuffle, ]

            predict_pipe <- pipe(.function = custom_pipe_predict, shuffle = shuffle)

            return(list(train = train, pipe = predict_pipe))
        }

        custom_pipe_predict <- function(data, shuffle) {
            return(data[shuffle, ])
        }

        r <- generate_model_function(segment(.segment = custom_pipe))
        train <- r$train
        test <- r$test
        full_pipe <- r$full_pipe
        rm(custom_pipe, custom_pipe_predict)

        result <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                 package_name = library_name,
                                                 libraries = libs,
                                                 tar_file = tar_file_name,
                                                 may_overwrite_tar_file = T, verbose = F)
        expect_true(object = result, info = "Build function returned a success")
        expect_true(file.exists(tar_file_name))

        install.packages(tar_file_name, repos = NULL, verbose = F, quiet = T)

        lib_predictions <- get_library_predictions(library_name = library_name, test = test)
        lib_df_predictions <- predict_model(test)
        function_predictions <- invoke(full_pipe, test)

        expect_equal(lib_predictions$one_lm_1, function_predictions$one_lm_1)
        expect_equal(lib_df_predictions$one_lm_1, function_predictions$one_lm_1)
        remove.packages(pkgs = library_name)
        expect_true(file.remove(tar_file_name))
    })
})

# This test takes a VERY VERY LONG time to run, since it has to install dependencies.
describe("build_docker()", {

    it("can build a docker image around a previously packaged model image", {
        connectivity <- F
        tryCatch({
            curl::nslookup("www.r-project.org")
            connectivity <- T
        }, error = function(e) return())

        if(connectivity && is_docker_running()){
            r <- generate_model_function()
            train <- r$train
            test <- r$test
            full_pipe <- r$full_pipe
            tar_file_name <- "tmp_test_package.tar.gz"
            library_name <- "test.package"
            image_name <- "model.image"
            process_name <- "datapiper.test"
            libs <- c("datapiper")

            package_result <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                             package_name = library_name,
                                                             libraries = libs,
                                                             tar_file = tar_file_name,
                                                             may_overwrite_tar_file = T, verbose = F)
            expect_true(object = package_result, info = "Build function returned a success")

            tryCatch({
                result <- build_docker(model_library_file = tar_file_name, package_name = library_name, libraries = libs,
                                       docker_image_name = image_name, may_overwrite_docker_image = T)
                expect_true(object = result, info = "Build function returned a success")

                docker_prediction <- test_docker(data = test, image_name = image_name, process_name = process_name,
                                                 package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)

                install.packages(tar_file_name, repos = NULL, verbose = F, quiet = T)
                lib_prediction <- get_library_predictions(library_name = library_name, test = test)
                pipe_prediction <- invoke(full_pipe, test)

                mean_abs_error_pipe <- mean(abs(unlist(pipe_prediction - docker_prediction)))
                expect_lte(mean_abs_error_pipe / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

                mean_abs_error_lib <- mean(abs(unlist(lib_prediction - docker_prediction)))
                expect_lte(mean_abs_error_lib / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

                delete_image(image_name = image_name)
                remove.packages(pkgs = library_name)
            }, error = function(e) {
                error_message <- as.character(e)
                if(grepl(x = error_message, pattern = "is_docker_running() is not TRUE", fixed = T)) {
                    warning(error_message)
                } else {
                    expect_true(F, info = "Error building docker image")
                }
            })

            expect_true(file.remove(tar_file_name))
        } else {
            warning("Error: no internet connectivity, can't test build_docker")
        }
    })

    it("can pass additional build commands to docker", {

    })
})
