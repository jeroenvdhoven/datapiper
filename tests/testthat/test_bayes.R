context("Bayesian optimization")
p_1 <- datapiper::train_pipeline(
    segment(.segment = datapiper::pipe_categorical_filter, threshold_function = function(x) 2, response = "x"),
    segment(.segment = datapiper::pipe_remove_single_value_columns, na_function = is.na),
    segment(.segment = datapiper::pipe_one_hot_encode),
    segment(.segment = datapiper::pipe_impute, exclude_columns = "x", columns = c("m", "m2"), type = "mean")
)
p_2 <- datapiper::train_pipeline(
    segment(.segment = p_1),
    segment(.segment = datapiper::pipe_scaler, retransform_columns = "a", exclude_columns = "x")
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

describe("find_model_through_bayes()", {
    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]

    it("runs without errors on basic settings", {
        r <- ctest_for_no_errors(
            error_message = "find_model_through_bayes does not run without errors on basic settings",
            to_eval = datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                            preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                            models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                            target_metric = "rmse", seed = 1, prepend_data_checker = F))
    })

    it("return a dataframe containing the results", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
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
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)

        pipe <- p_1(train)
        model <- model_xgb$.train(pipe$train, nrounds = nrounds)
        train_rmse <- m_1(model_xgb$.predict(model, pipe$train), train$x)
        expect_equivalent(train_rmse, r$train_rmse)

        test_rmse <- m_1(model_xgb$.predict(model, invoke(pipe$pipe, test)), test$x)
        expect_equivalent(test_rmse, r$test_rmse)
    })

    it("can use multiple pipelines", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1, "two" = p_2), higher_is_better = F,
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 2)
        expect_equal(r$.id, c("one_xgb", "two_xgb"))
        expect_equal(r$.preprocess_pipe[[1]], p_1(train)$pipe)
        expect_equal(r$.preprocess_pipe[[2]], p_2(train)$pipe)
    })

    it("also stores post_pipes if requested by pipelines", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                                 preprocess_pipes = list("one" = p_1, "two" = p_2), higher_is_better = F,
                                                 models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                                 target_metric = "rmse", seed = 1, prepend_data_checker = F)
        expect_true(".post_pipe" %in% colnames(r))
        expect_equal(r$.post_pipe[[1]], NULL)
        expect_equal(r$.post_pipe[[2]], p_2(train)$post_pipe)
    })

    it("can use multiple models", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("xgb" = model_xgb, "forest" = model_forest), metrics = list("rmse" = m_1),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 2)
        expect_equal(r$.id, c("one_xgb", "one_forest"))
        expect_equal(r$.train[[1]], model_xgb$.train)
        expect_equal(r$.train[[2]], model_forest$.train)

        expect_equal(r$.predict[[1]], model_xgb$.predict)
        expect_equal(r$.predict[[2]], model_forest$.predict)
    })

    it("can use multiple metrics", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("xgb" = model_xgb), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 1)
        expect_false(any(!c("train_rmsle", "test_rmsle") %in% colnames(r)))
    })

    it("can use multiple parameter sets", {
        ntree = c(100, 300, 400)
        nodesize = c(1,5,20)
        model_forest_w_params <- datapiper::find_template_formula_and_data(
            response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
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

        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)

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

    # TODO
    it("can learn from experiments to generate likely new good configurations", {
        ntree = c(100, 300, 400, 50, 10, 600)
        nodesize = c(1,5,20, 10, 3)
        model_forest_w_params <- datapiper::find_template_formula_and_data(
            response = "x", training_function = randomForest::randomForest, ntree = ntree, nodesize = nodesize)

        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1),
                                   N_init = 5, N_experiment = 20, higher_is_better = F,
                                   models = list("forest" = model_forest_w_params), metrics = list("rmse" = m_1, "rmsle" = m_2),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = F)
        expect_equal(nrow(r), 25)

        params <- bind_rows(r$params)
        expect_equal(params, unique(params), info = "Only unique parameter combinations are kept")

        given_params <- expand.grid(ntree = ntree, nodesize = nodesize)
        expect_false(any(!params$ntree %in% given_params$ntree))
        expect_false(any(!params$nodesize %in% given_params$nodesize))
    })

    it("can prepend pipeline_check to all pipelines", {
        r <- datapiper::find_model_through_bayes(train = train, test = test, response = "x", verbose = F,
                                   preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                   models = list("forest" = model_xgb), metrics = list("rmse" = m_1),
                                   target_metric = "rmse", seed = 1, prepend_data_checker = T)

        pipe <- r$.preprocess_pipe[[1]]

        no_response <- select(dataset1, -x)
        expect_equal(object = invoke(pipe, no_response), expected = select(invoke(pipe, dataset1), -x))

        no_required_column <- select(dataset1, -y)
        expect_error(object = invoke(pipe, no_required_column), regexp = "not present while expected")
    })

    it("only checks if the response is present after piping", {
        p_3 <- datapiper::train_pipeline(
            segment(.segment = p_1),
            segment(.segment = pipe_mutate, Target = "x"),
            segment(.segment = pipe_function, f = standard_column_names)
        )

        r <- ctest_for_no_errors(
            error_message = "find_model_through_bayes does not check for response after piping",
            to_eval = find_model_through_bayes(train = dataset1, test = test, response = "target", verbose = F,
                                 preprocess_pipes = list("one" = p_3), higher_is_better = F,
                                 models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                 target_metric = "rmse", seed = 1, prepend_data_checker = F))
    })

    it("can save generated models if asked", {
        r_without <- find_model_through_bayes(train = dataset1, test = test, response = "x", verbose = F,
                                preprocess_pipes = list("one" = p_1), higher_is_better = F,
                                models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                                target_metric = "rmse", seed = 1, prepend_data_checker = F, save_model = F)
        expect_false(".model" %in% colnames(r_without))

        r_with <- find_model_through_bayes(train = dataset1, test = test, response = "x", verbose = F,
                             preprocess_pipes = list("one" = p_1), higher_is_better = F,
                             models = list("xgb" = model_xgb), metrics = list("rmse" = m_1),
                             target_metric = "rmse", seed = 1, prepend_data_checker = F, save_model = T)
        expect_true(".model" %in% colnames(r_with))

        model <- r_with$.model[[1]]
        expect_true("xgb.Booster" %in% class(model))
    })
})

