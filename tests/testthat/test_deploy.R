get_library_predictions <- function(library_name, test){
    library(library_name, character.only = T)
    test_json <- jsonlite::toJSON(x = test, dataframe = "rows",
                                  Date = "ISO8601", POSIXt = "ISO8601", factor = "string", complex = "list", raw = "base64",
                                  null = "null", na = "null", digits = 8, pretty = F)

    return(predict_model(test_json))
}

generate_model_function <- function(){
    pipe <- datapiper::pipeline(
        segment(.segment = pipeline_select, "x", "a", "b", "c", "s")
    )
    m <- datapiper::find_template_formula_and_data(response = "x", training_function = lm)

    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]

    find_model_result <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                               preprocess_pipes = list("one" = pipe),
                                               models = list("lm" = m), metrics = list("rmse" = util_RMSE),
                                               parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)

    full_pipe <- find_best_models(train = train, find_model_result = find_model_result,
                                  metric = "test_rmse", higher_is_better = F)$.predict

    return(list(
        train = train,
        test = test,
        full_pipe = full_pipe
    ))
}

describe("build_model_package()", {
    it("can build a package around a model pipeline", {
        r <- generate_model_function()
        train <- r$train
        test <- r$test
        full_pipe <- r$full_pipe
        tar_file_name <- "tmp_test_package.tar.gz"
        library_name <- "test.package"

        result <- datapiper::build_model_package(model_function = full_pipe,
                                                 package_name = library_name,
                                                 libraries = c("dplyr", "magrittr", "datapiper"),
                                                 tar_file = tar_file_name,
                                                 may_overwrite_tar_file = T)
        expect_true(object = result, info = "Build function returned a success")
        expect_true(file.exists(tar_file_name))

        install.packages(tar_file_name, repos = NULL, type = "source")

        lib_predictions <- get_library_predictions(library_name = library_name, test = test)
        function_predictions <- full_pipe(test)

        expect_equal(lib_predictions$one_lm_1, function_predictions$one_lm_1)
        remove.packages(pkgs = library_name)
        expect_true(file.remove(tar_file_name))
    })
})

describe("build_docker()", {

    it("can build a docker image around a previously packaged model image", {
        connectivity <- F
        tryCatch({
            curl::nslookup("www.r-project.org")
            connectivity <- T
        }, error = function(e) warning(e))
return()
        if(connectivity){
            r <- generate_model_function()
            train <- r$train
            test <- r$test
            full_pipe <- r$full_pipe
            tar_file_name <- "tmp_test_package.tar.gz"
            library_name <- "test.package"
            image_name <- "model.image"
            process_name <- "datapiper.test"

            package_result <- datapiper::build_model_package(model_function = full_pipe,
                                                     package_name = library_name,
                                                     libraries = c("dplyr", "magrittr", "datapiper"),
                                                     tar_file = tar_file_name, prediction_precision = 12,
                                                     may_overwrite_tar_file = T)
            expect_true(object = package_result, info = "Build function returned a success")

            result <- build_docker(model_library_file = tar_file_name, package_name = library_name, libraries = c("dplyr", "xgboost"),
                         docker_image_name = image_name, may_overwrite_docker_image = T)
            expect_true(object = result, info = "Build function returned a success")

            docker_prediction <- test_docker(data = test, image_name = image_name, process_name = process_name,
                                             package_name = library_name, batch_size = 1e3, ping_time = 5, verbose = T)
            lib_prediction <- get_library_predictions(library_name = library_name, test = test)
            pipe_prediction <- full_pipe(test)

            mean_abs_error_pipe <- mean(abs(unlist(pipe_prediction - docker_prediction)))
            expect_lte(mean_abs_error_pipe / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

            mean_abs_error_lib <- mean(abs(unlist(lib_prediction - docker_prediction)))
            expect_lte(mean_abs_error_lib / mean(test$x), 1e-5, label = "Prediction error from docker differ too much from pipe prediction")

            delete_image(image_name = image_name)
            remove.packages(pkgs = library_name)
            expect_true(file.remove(tar_file_name))
        } else {
            expect_error(c, info = "Error: no internet connectivity, can't test build_docker")
        }
    })

    it("can pass additional build commands to docker", {

    })
})
