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
            # description_contents <- scan(file = description_file, what = "character", sep = "\n", quiet = T)
            # for(lib in libs) expect_true(any(grepl(pattern = lib, x = description_contents)), info = paste(lib, "not found in description file"))

            unlink(x = extract_dir, recursive = T)
        })

        it("also allows you to create a plumber endpoint", {
            if("plumber" %in% list.files(.libPaths())) {
                expect_true(exists("create_plumber_endpoint"))
                plumber_endpoint <- create_plumber_endpoint()

                expect_true("plumber" %in% class(plumber_endpoint))

                model_endpoint <- plumber_endpoint$endpoints$`__no-preempt__`[[1]]

                model_path <- paste0("/", library_name, "/predict_model")
                expect_equal(object = model_endpoint$path, expected = model_path)
            } else {
                warning("Did not find plumber in your library, not testing the creation of a plumber endpoint")
            }
        })

        remove.packages(pkgs = library_name)
        expect_true(file.remove(tar_file_name))
    })

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

evaluate_images <- function(docker_prediction, docker_single_prediction, test, image_name, process_name, library_name, tar_file_name, full_pipe) {

    install.packages(tar_file_name, repos = NULL, verbose = F, quiet = T)
    lib_prediction <- get_library_predictions(library_name = library_name, test = test)
    lib_single_prediction <- unlist(lib_prediction[1,])

    pipe_prediction <- invoke(full_pipe, test)
    pipe_single_prediction <- unlist(pipe_prediction[1,])

    # Multi predictions
    mean_abs_error_pipe <- mean(abs(unlist(pipe_prediction - docker_prediction)))
    expect_lte(mean_abs_error_pipe / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

    mean_abs_error_lib <- mean(abs(unlist(lib_prediction - docker_prediction)))
    expect_lte(mean_abs_error_lib / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

    # Single predictions
    mean_abs_error_pipe_single <- mean(abs(unlist(pipe_single_prediction - docker_single_prediction)))
    expect_lte(mean_abs_error_pipe_single / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

    mean_abs_error_lib_single <- mean(abs(unlist(lib_single_prediction - docker_single_prediction)))
    expect_lte(mean_abs_error_lib_single / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")
}

# This test takes a VERY VERY LONG time to run, since it has to install dependencies.
describe("build_docker()", {

    it("can build a docker image around a previously packaged model image using opencpu", {
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
            image_name <- "model.image.opencpu"
            process_name <- "datapiper.test"
            libs <- c("datapiper")
            type <- "opencpu"

            package_result <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                             package_name = library_name,
                                                             libraries = libs,
                                                             tar_file = tar_file_name,
                                                             may_overwrite_tar_file = T, verbose = F)
            expect_true(object = package_result, info = "Build function returned a success")

            tryCatch({
                result <- build_docker(model_library_file = tar_file_name, package_name = library_name, libraries = libs,
                                       docker_image_type = type,
                                       docker_image_name = image_name, may_overwrite_docker_image = T)

                docker_prediction <- test_docker(data = test, image_name = image_name, process_name = process_name,
                                                 package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)
                docker_single_prediction <- test_docker(data = test[1,], image_name = image_name, process_name = process_name,
                                                        package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)

                expect_true(object = result, info = "Build function returned a success")

                evaluate_images(docker_prediction = docker_prediction, docker_single_prediction = docker_single_prediction,
                                test = test, image_name = image_name, process_name = process_name, library_name = library_name,
                                tar_file_name = tar_file_name, full_pipe = full_pipe)

                delete_image(image_name = image_name)
                remove.packages(pkgs = library_name)
            }, error = function(e) {
                error_message <- as.character(e)
                if(grepl(x = error_message, pattern = "is_docker_running() is not TRUE", fixed = T)) {
                    warning(error_message)
                } else {
                    expect_true(F, info = "Error building docker image", label = as.character(e))
                }
            })

            expect_true(file.remove(tar_file_name))
        } else {
            warning("Error: no internet connectivity, can't test build_docker")
        }
    })

    it("can build a docker image around a previously packaged model image using plumber", {
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
            image_name <- "model.image.plumber"
            process_name <- "datapiper.test"
            libs <- c("datapiper")
            type <- "plumber"

            package_result <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                             package_name = library_name,
                                                             libraries = libs,
                                                             tar_file = tar_file_name,
                                                             may_overwrite_tar_file = T, verbose = F)
            expect_true(object = package_result, info = "Build function returned a success")

            tryCatch({
                # http://localhost:8004/test.package/predict_model?input=[{%22x%22:1,%22a%22:1,%22b%22:0,%22c%22:2,%22y%22:%22b%22,%22s%22:%22A%22,%22m%22:5,%22z%22:%22A%22,%22boolean%22:true,%22z2%22:%22A%22},{%22x%22:2,%22a%22:4,%22b%22:0.6931,%22c%22:8,%22y%22:%22c%22,%22s%22:%22A%22,%22m2%22:3,%22z%22:%22A%22,%22boolean%22:true,%22z2%22:%22A%22}]
                result <- build_docker(model_library_file = tar_file_name, package_name = library_name, libraries = libs,
                                       docker_image_type = type,
                                       docker_image_name = image_name, may_overwrite_docker_image = T)
                docker_prediction <- test_docker(data = test, image_name = image_name, process_name = process_name,
                                                 docker_image_type = type, port = 8004,
                                                 package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)
                docker_single_prediction <- test_docker(data = test[1,], image_name = image_name, process_name = process_name,
                                                        docker_image_type = type, port = 8004,
                                                        package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)

                expect_true(object = result, info = "Build function returned a success")

                evaluate_images(docker_prediction = docker_prediction, docker_single_prediction = docker_single_prediction,
                                test = test, image_name = image_name, process_name = process_name, library_name = library_name,
                                tar_file_name = tar_file_name, full_pipe = full_pipe)

                delete_image(image_name = image_name)
                remove.packages(pkgs = library_name)
            }, error = function(e) {
                error_message <- as.character(e)
                if(grepl(x = error_message, pattern = "is_docker_running() is not TRUE", fixed = T)) {
                    warning(error_message)
                } else {
                    expect_true(F, info = "Error building docker image", label = as.character(e))
                }
            })

            expect_true(file.remove(tar_file_name))
        } else {
            warning("Error: no internet connectivity, can't test build_docker")
        }
    })


    it("can build a docker image around multiple previously packaged model image using plumber", {
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
            library_name_1 <- "test.package.1"
            library_name_2 <- "test.package.2"
            tar_file_name_1 <- paste0("tmp_", library_name_1, ".tar.gz")
            tar_file_name_2 <- paste0("tmp_", library_name_2, ".tar.gz")
            image_name <- "model.image.plumber"
            process_name <- "datapiper.test"
            libs <- c("datapiper")
            type <- "plumber"

            package_result_1 <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                             package_name = library_name_1,
                                                             libraries = libs,
                                                             tar_file = tar_file_name_1,
                                                             may_overwrite_tar_file = T, verbose = F)
            expect_true(object = package_result_1, info = "Build function returned a success")
            package_result_2 <- datapiper::build_model_package(trained_pipeline = full_pipe,
                                                               package_name = library_name_2,
                                                               libraries = libs,
                                                               tar_file = tar_file_name_2,
                                                               may_overwrite_tar_file = T, verbose = F)
            expect_true(object = package_result_2, info = "Build function returned a success")

            tryCatch({
                # http://localhost:8004/test.package/predict_model?input=[{%22x%22:1,%22a%22:1,%22b%22:0,%22c%22:2,%22y%22:%22b%22,%22s%22:%22A%22,%22m%22:5,%22z%22:%22A%22,%22boolean%22:true,%22z2%22:%22A%22},{%22x%22:2,%22a%22:4,%22b%22:0.6931,%22c%22:8,%22y%22:%22c%22,%22s%22:%22A%22,%22m2%22:3,%22z%22:%22A%22,%22boolean%22:true,%22z2%22:%22A%22}]
                result <- build_docker(model_library_file = c(tar_file_name_1, tar_file_name_2),
                                       package_name = c(library_name_1, library_name_2), libraries = libs,
                                       docker_image_type = type,
                                       docker_image_name = image_name, may_overwrite_docker_image = T)

                validate <- function(libname, tarname) {
                    docker_prediction <- test_docker(data = test, image_name = image_name, process_name = process_name,
                                                     docker_image_type = type, port = 8004,
                                                     package_name = libname, batch_size = 1e3, ping_time = 5, verbose = T)
                    docker_single_prediction <- test_docker(data = test[1,], image_name = image_name, process_name = process_name,
                                                            docker_image_type = type, port = 8004,
                                                            package_name = libname, batch_size = 1e3, ping_time = 5, verbose = T)

                    expect_true(object = result, info = "Build function returned a success")

                    evaluate_images(docker_prediction = docker_prediction, docker_single_prediction = docker_single_prediction,
                                    test = test, image_name = image_name, process_name = process_name, library_name = libname,
                                    tar_file_name = tarname, full_pipe = full_pipe)
                }
                validate(libname = library_name_1, tarname = tar_file_name_1)
                validate(libname = library_name_2, tarname = tar_file_name_2)

                delete_image(image_name = image_name)
                remove.packages(pkgs = library_name_1)
                remove.packages(pkgs = library_name_2)
            }, error = function(e) {
                error_message <- as.character(e)
                if(grepl(x = error_message, pattern = "is_docker_running() is not TRUE", fixed = T)) {
                    warning(error_message)
                } else {
                    expect_true(F, info = "Error building docker image", label = as.character(e))
                }
            })

            expect_true(file.remove(tar_file_name_1))
            expect_true(file.remove(tar_file_name_2))
        } else {
            warning("Error: no internet connectivity, can't test build_docker")
        }
    })
})
