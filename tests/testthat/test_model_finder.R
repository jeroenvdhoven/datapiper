p_1 <- datapiper::train_pipeline(
    segment(.segment = datapiper::feature_categorical_filter, threshold_function = function(x) 2, response = "x"),
    segment(.segment = datapiper::remove_single_value_columns, na_function = is.na),
    segment(.segment = datapiper::feature_one_hot_encode),
    segment(.segment = datapiper::impute_all, exclude_columns = "x", columns = c("m", "m2"), type = "mean")
)
p_2 <- datapiper::train_pipeline(
    segment(.segment = p_1),
    segment(.segment = datapiper::feature_scaler, exclude_columns = "x")
)
m_1 <- util_RMSE
m_2 <- util_RMSLE

nrounds <- 10
model_xgb <- find_xgb(response = "x", nrounds = nrounds)
model_forest <- datapiper::find_template_formula_and_data(response = "x", training_function = randomForest::randomForest)

ctest_has_correct_content <- function(column, outer_class, inner_class) {
    expect_true(outer_class %in% class(column))
    if(outer_class == "list") {
        expect_true(inner_class %in% class(column[[1]]))
    }
    return(invisible(T))
}

ctest_best_model_result <- function(test, res, n_models = 1) {
    expect_true(is.data.frame(res))
    expect_equal(ncol(res), n_models)
    expect_equal(nrow(res), nrow(test))

    for(i in seq_len(n_models)) expect_true(is.numeric(res[[i]]), info = paste("Column", i, "is not numeric"))
    expect_named(res)
}

describe("find_model()", {
    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]

    it("runs without errors on basic settings", {
        r <- ctest_for_no_errors(
            error_message = "find_model does not run without errors on basic settings",
            to_eval = datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                            preprocess_pipes = list("one" = p_1),
                                            models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                            parameter_sample_rate = 1, seed = 1, prepend_data_checker = F))
    })

    it("return a dataframe containing the results", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)
        expect_true(is.data.frame(r))
        expect_equivalent(nrow(r), 1)
        expect_named(object = r, c(".train", ".predict", ".id", "params", ".preprocess_pipe", "train_rmse", "test_rmse"), ignore.order = T)
        expect_true(is.list(r))

        ctest_has_correct_content(column = r$params, "list", "data.frame")
        ctest_has_correct_content(column = r$.train, "list", "function")
        ctest_has_correct_content(column = r$.predict, "list", "function")
        ctest_has_correct_content(column = r$.preprocess_pipe, "list", "pipeline")

        ctest_has_correct_content(column = r$.id, "character")
        ctest_has_correct_content(column = r$train_rmse, "numeric")
        ctest_has_correct_content(column = r$test_rmse, "numeric")

        expect_equal(formalArgs(r$.train[[1]]), c("data", "..."))
        expect_equal(length(formalArgs(r$.predict[[1]])), 2)

        expect_equal(r$.id, "one_xgb")
    })

    it("trains models as expected", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)

        pipe <- p_1(train)
        model <- model_xgb$.train(pipe$train, nrounds = nrounds)
        train_rmse <- m_1(model_xgb$.predict(model, pipe$train), train$x)
        expect_equivalent(train_rmse, r$train_rmse)

        test_rmse <- m_1(model_xgb$.predict(model, invoke(pipe$pipe, test)), test$x)
        expect_equivalent(test_rmse, r$test_rmse)
    })

    it("can use multiple pipelines", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1, "two" = p_2),
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 2)
        expect_equal(r$.id, c("one_xgb", "two_xgb"))
        expect_equal(r$.preprocess_pipe[[1]], p_1(train)$pipe)
        expect_equal(r$.preprocess_pipe[[2]], p_2(train)$pipe)
    })

    it("can use multiple models", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("xgb" = model_xgb, "forest" = model_forest), metrics = list("rmse" = m_1),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 2)
        expect_equal(r$.id, c("one_xgb", "one_forest"))
        expect_equal(r$.train[[1]], model_xgb$.train)
        expect_equal(r$.train[[2]], model_forest$.train)

        expect_equal(r$.predict[[1]], model_xgb$.predict)
        expect_equal(r$.predict[[2]], model_forest$.predict)
    })

    it("can use multiple metrics", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 1)
        expect_false(any(!c("train_rmsle", "test_rmsle") %in% colnames(r)))
    })

    it("can use multiple parameter sets", {
        ntree = c(100, 300, 400)
        nodesize = c(1,5,20)
        model_forest_w_params <- datapiper::find_template_formula_and_data(
            response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 9)

        params <- bind_rows(r$params)
        given_params <- expand.grid(ntree = ntree, nodesize = nodesize)
        expect_equal(params, given_params)
    })

    it("can use parameters for a model", {
        ntree = c(100)
        nodesize = c(1)
        model_forest_w_params <- datapiper::find_template_formula_and_data(
            response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)

        pipe <- p_1(train)
        used_params <- as.list(r$params[[1]])
        used_params$data <- pipe$train

        set.seed(1)
        model <- do.call(what = model_forest_w_params$.train, args = used_params)
        expect_equal(model$ntree, ntree)
        train_rmse <- m_1(model_xgb$.predict(model, pipe$train), train$x)
        expect_equivalent(train_rmse, r$train_rmse[1])

        test_rmse <- m_1(model_xgb$.predict(model, invoke(pipe$pipe, test)), test$x)
        expect_equivalent(test_rmse, r$test_rmse[1])
    })

    it("can subsample the parameter space", {
        ntree = c(100, 300, 400)
        nodesize = c(1,5,20)
        model_forest_w_params <- datapiper::find_template_formula_and_data(
            response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   parameter_sample_rate = .3333, seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 3)

        params <- bind_rows(r$params)
        expect_equal(params, unique(params), info = "Only unique parameter combinations are kept")

        given_params <- expand.grid(ntree = ntree, nodesize = nodesize)
        expect_false(any(!params$ntree %in% given_params$ntree))
        expect_false(any(!params$nodesize %in% given_params$nodesize))
    })

    it("can prepend pipeline_check to all pipelines", {
        r <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   models = list("forest" = model_xgb), metrics = list("rmse" = m_1),
                                   parameter_sample_rate = .3333, seed = 1, prepend_data_checker = T)

        pipe <- r$.preprocess_pipe[[1]]

        no_response <- select(dataset1, -x)
        expect_equal(object = select(invoke(pipe, no_response), -x), expected = select(invoke(pipe, dataset1), -x))

        no_required_column <- select(dataset1, -y)
        expect_error(object = invoke(pipe, no_required_column), regexp = "not present while expected")
    })

    it("only checks if the response is present after piping", {
        p_3 <- datapiper::train_pipeline(
            segment(.segment = p_1),
            segment(.segment = pipeline_mutate, Target = "x"),
            segment(.segment = pipeline_function, f = standard_column_names)
        )

        r <- ctest_for_no_errors(
            error_message = "find_model does not check for response after piping",
            to_eval = find_model(train = dataset1, test = test, response = "target", verbose = F,
                                 preprocess_pipes = list("one" = p_3),
                                 models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                 parameter_sample_rate = 1, seed = 1, prepend_data_checker = F))
    })

    it("can save generated models if asked", {
        r_without <- find_model(train = dataset1, test = test, response = "x", verbose = F,
                        preprocess_pipes = list("one" = p_1),
                        models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                        parameter_sample_rate = 1, seed = 1, prepend_data_checker = F, save_model = F)
        expect_false(".model" %in% colnames(r_without))

        r_with <- find_model(train = dataset1, test = test, response = "x", verbose = F,
                        preprocess_pipes = list("one" = p_1),
                        models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                        parameter_sample_rate = 1, seed = 1, prepend_data_checker = F, save_model = T)
        expect_true(".model" %in% colnames(r_with))

        model <- r_with$.model[[1]]
        expect_true("xgb.Booster" %in% class(model))
    })
})




describe("find_best_models()", {
    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]

    ntree = c(100, 300, 400)
    nodesize = c(1,5,20)
    model_forest_w_params <- datapiper::find_template_formula_and_data(
        response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

    find_model_result <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                               preprocess_pipes = list("one" = p_1),
                                               models = list("forest" = model_forest_w_params, "xgb" = model_xgb), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                               parameter_sample_rate = 1, seed = 1, prepend_data_checker = F)

    it("runs without errors on basic settings", {
        r <- ctest_for_no_errors(to_eval = find_best_models(train = train, find_model_result = find_model_result,
                                                            metric = "test_rmse", higher_is_better = F),
                                 error_message = "find_best_models did not run on basic settings")
    })

    it("can find the best model given the result of find_model() and train it", {
        r <- find_best_models(train = train, find_model_result = find_model_result,
                              metric = "test_rmse", higher_is_better = F, top_n = 1, per_model = F)
        models <- r[[1]]$args$models
        expect_true(is.pipeline(r))
        expect_named(models, paste0(find_model_result$.id[which.min(find_model_result$test_rmse)], "_1"))
        ctest_has_correct_content(column = models, "list", "xgb.Booster")
    })

    it("returns a function that runs the full pipeline and the models", {
        r <- find_best_models(train = train, find_model_result = find_model_result, metric = "test_rmse", higher_is_better = F, top_n = 1)
        predictions <- invoke(r, test)
        ctest_best_model_result(test = test, res = predictions)
    })

    it("can choose different metrics for evaluation models", {
        dummy_find_model_result <- find_model_result
        dummy_find_model_result$test_dummy <- seq_len(nrow(dummy_find_model_result))
        r <- find_best_models(train = train, find_model_result = dummy_find_model_result, metric = "test_dummy", higher_is_better = F, top_n = 1)

        models <- r[[1]]$args$models
        ctest_has_correct_content(column = models, "list", "randomForest")
    })

    it("can set if the metric should be high or low", {
        r_xgb <- find_best_models(train = train, find_model_result = find_model_result,
                                 metric = "test_rmse", higher_is_better = F, top_n = 1)
        ctest_has_correct_content(column = r_xgb[[1]]$args$models, "list", "xgb.Booster")

        r_rf <- find_best_models(train = train, find_model_result = find_model_result,
                                 metric = "test_rmse", higher_is_better = T, top_n = 1)
        ctest_has_correct_content(column = r_rf[[1]]$args$models, "list", "randomForest")
    })

    it("can select multiple models for training", {
        r <- find_best_models(train = train, find_model_result = find_model_result,
                              metric = "test_rmse", higher_is_better = F, top_n = 3)

        expect_equal(length(r[[1]]$args$models), 3)
        predictions <- invoke(r, test)
        ctest_best_model_result(test = test, res = predictions, n_models = 3)
    })

    it("can select models per model/pipeline combination", {
        r <- find_best_models(train = train, find_model_result = find_model_result, per_model = T,
                              metric = "test_rmse", higher_is_better = F, top_n = 3)

        models <- r[[1]]$args$models
        expect_equal(length(models), 4)
        predictions <- invoke(r, test)
        ctest_best_model_result(test = test, res = predictions, n_models = 4)

        expect_named(predictions, c("one_xgb_1", "one_forest_1", "one_forest_2", "one_forest_3"), ignore.order = T)
        correct_models <- purrr::map2_lgl(.x = r$models, .y = c("xgb.Booster", "randomForest", "randomForest", "randomForest"), .f = function(x,y){
            return(any(class(x) == y))
        })
        expect_false(any(!correct_models), info = "All models have the correct type")
    })

    it("can generate individual predictions or aggregate them", {
        set.seed(1)
        r_mean <- find_best_models(train = train, find_model_result = find_model_result, per_model = F,
                                   metric = "test_rmse", higher_is_better = F, top_n = 3, aggregate_func = mean)

        set.seed(1)
        r_plain <- find_best_models(train = train, find_model_result = find_model_result, per_model = F,
                                    metric = "test_rmse", higher_is_better = F, top_n = 3, aggregate_func = NA)

        pred_mean <- invoke(r_mean, test)
        pred_plain <- invoke(r_plain, test)

        ctest_best_model_result(test = test, res = pred_mean, n_models = 1)
        meaned <- data_frame(value = apply(pred_plain, 1 , mean))

        expect_equivalent(meaned, pred_mean)
    })

    it("can use the .model column if present", {
        set.seed(1)
        find_model_result <- datapiper::find_model(train = train, test = test, response = "x", verbose = F,
                                                   preprocess_pipes = list("one" = p_1),
                                                   models = list("forest" = model_forest_w_params, "xgb" = model_xgb),
                                                   metrics = list("rmse" = m_1, "rmsle" = m_2),
                                                   parameter_sample_rate = 1, seed = 1, prepend_data_checker = F, save_model = T)
        find_model_result <- arrange(find_model_result, test_rmse)
        N <- 3
        r_plain <- find_best_models(train = train, find_model_result = find_model_result, per_model = F,
                                   metric = "test_rmse", higher_is_better = F, top_n = N, aggregate_func = NA)
        r_plain <- r_plain[[1]]
        pred_plain <- invoke(r_plain, test)

        original_models <- find_model_result$.model[seq_len(N)]
        original_pipes <- find_model_result$.preprocess_pipe[seq_len(N)]
        original_predict <- find_model_result$.predict[seq_len(N)]

        ctest_best_model_result(test = test, res = pred_plain, n_models = N)

        invoke_model <- function(model, pred_func, trained_pipeline) pred_func(model, invoke(trained_pipeline, test))
        for(i in seq_len(N)) {
            find_model_pred <- invoke_model(model = original_models[[i]], pred_func = original_predict[[i]], trained_pipeline = original_pipes[[i]])
            expect_equivalent(object = unlist(pred_plain[, i]), expected = find_model_pred,
                              info = paste("Model", i, "produces different results through find_model and find_best_models"))
        }
    })

    it("names its pipeline elements by default", {
        r_no_aggregation <- find_best_models(train = train, find_model_result = find_model_result,
                              metric = "test_rmse", higher_is_better = F)
        expect_named(r_no_aggregation, "model")

        r_aggregation <- find_best_models(train = train, find_model_result = find_model_result,
                                             metric = "test_rmse", higher_is_better = F, top_n = 3, aggregate_func = mean)
        expect_named(r_aggregation, c("model", "aggregate"))

    })
})
