#' Find fitting models and test them using given metrics on the test dataset
#'
#' @param train The training dataset
#' @param test The testing dataset
#' @param response The response column as a string
#' @param models A list of models. Each model should be a list, containing at least a training function \code{.train} and a \code{.predict} function, plus named
#' vectors of parameters to explore.
#'
#' The \code{.train} function has to take a \code{data} argument that stores the training data and a \code{...} argument for the parameters.
#' The \code{.predict} function needs to take two arguments, where the first is the model and the second the new dataset.
#'
#' If a parameter only takes a single value, you can use a vector to store options. Otherwise use a list.
#'
#' You can use \code{\link{model_trainer}} as a wrapper for this list. It will also test your inputs.
#' @param metrics A list of metrics (functions) that need to be calculated on the train and test response and predictions
#' @param parameter_sample_rate Optional parameter. If set in the range \code{(0,1])}, it will be used to sample the possible combinations of parameters
#' @param seed Random seed to set each time before a model is trained
#' @param preprocess_pipes List of preprocessing pipelines generated using \code{\link{pipeline}}.
#' @param prepend_data_checker Flag indicating if \code{\link{pipeline_check}} should be prepended before all pipelines.
#' @param on_missing_column See \code{\link{pipeline_check}} for details.
#' @param on_extra_column See \code{\link{pipeline_check}} for details.
#' @param on_type_error See \code{\link{pipeline_check}} for details.
#' @param verbose Should intermediate updates be printed.
#'
#' @return A dataframe containing the training function, a list of parameters used to train the function, and one column for each metric / dataset combination.
#' @export
#' @importFrom purrr map_dbl map_lgl pmap_df
find_model <- function(train, test, response,
                       models, metrics, parameter_sample_rate = 1, seed = 1,
                       prepend_data_checker = T,
                       on_missing_column = c("error", "add")[1],
                       on_extra_column = c("remove", "error")[1],
                       on_type_error = c("ignore", "error")[1],
                       verbose = T,
                       preprocess_pipes = list(function(train, test) return(list(train = train, test = train, .predict = function(data) return(data))))
) {
    stopifnot(
        !missing(train), is.data.frame(train),
        !missing(test) , is.data.frame(test),
        is.character(response), length(response) == 1,
        response %in% colnames(train),
        response %in% colnames(test),
        is.logical(prepend_data_checker),
        is.list(models),
        parameter_sample_rate <= 1, parameter_sample_rate > 0,
        is.list(metrics), !any(!purrr::map_lgl(metrics, is.function)),
        is.list(preprocess_pipes), !any(!purrr::map_lgl(preprocess_pipes, is.function))
    )

    models_have_valid_elements <- purrr::map_lgl(models, function(m) {
        if(any(!c(".train", ".predict") %in% names(m))) return(F)

        are_functions <- purrr::map_lgl(m[c(".train", ".predict")], is.function)
        return(!any(!are_functions))
    })

    if(is.null(names(models))) model_names <- seq_along(models)
    else model_names <- names(models)

    if(is.null(names(preprocess_pipes))) pipe_names <- seq_along(preprocess_pipes)
    else pipe_names <- names(preprocess_pipes)

    if(any(!models_have_valid_elements)) stop("Error: all models must contain .train and .predict elements that are functions")
    if(!is.null(names(metrics))) metric_names <- names(metrics)
    else metric_names <- paste0("metric_", seq_along(metrics))

    res <- data_frame(.train = list(), .predict = list(), .id = "", params = list(), .preprocess_pipe = list())[0,]
    for(metric_name in metric_names) {
        res[paste0("train_", metric_name)] <- numeric(0)
        res[paste0("test_", metric_name)] <- numeric(0)
    }

    # Try each preprocessing pipe
    for(preprocess_index in seq_along(preprocess_pipes)){
        preprocess_pipe <- preprocess_pipes[[preprocess_index]]
        if(prepend_data_checker){
            preprocess_pipe <- datapiper::pipeline(
                segment(.segment = datapiper::pipeline_check, response = response,
                     on_missing_column = on_missing_column, on_extra_column = on_extra_column, on_type_error = on_type_error),
                segment(.segment = preprocess_pipe))
        }
        piped <- preprocess_pipe(train)
        piped_train <- piped$train
        pipe <- piped$.predict
        piped_test <- pipe(test)
        pipe_name <- pipe_names[preprocess_index]

        # Try each model
        for(model_index in seq_along(models)) {
            model <- models[[model_index]]
            model_name <- model_names[model_index]
            f_train <- model[[".train"]]
            f_predict <- model[[".predict"]]

            parameter_grid <- expand.grid(stringsAsFactors = F, model[!names(model) %in% c(".train", ".predict")])
            if(parameter_sample_rate < 1) {
                n_samples <-  ceiling(nrow(parameter_grid) * parameter_sample_rate)
                subsection <- sample.int(n = nrow(parameter_grid), size = n_samples, replace = F)
                parameter_grid <- parameter_grid[subsection,,drop = F]
            }

            if(nrow(parameter_grid) < 1) parameter_grid <- data_frame(1)[,0]
            parameter_grid <- as_data_frame(parameter_grid)

            # Try each combination of parameters
            for(r in seq_len(nrow(parameter_grid))) {
                update_message <- paste("\rComputing preprocess pipeline", preprocess_index, "/", length(preprocess_pipes), "model", model_index, "/", length(models), "iteration", r, "/", nrow(parameter_grid))
                if(verbose) cat("\r", update_message, sep = "")
                set.seed(seed)

                args <- list(data = piped_train)
                args <- c(args, unlist(parameter_grid[r,]))

                requested_arguments <- formalArgs(f_train)
                if(any(!names(args) %in% requested_arguments) && !"..." %in% requested_arguments) {
                    faulty_args <- names(args)[!names(args) %in% requested_arguments]
                    text_args <- paste0(collapse = ", ", faulty_args)
                    stop(paste0("Warning in preprocess pipeline ", preprocess_index, ", model ", model_index , ": arguments `", text_args, "` were not arguments of the provided .train function"))
                }

                model <- do.call(what = f_train, args = args)

                # Do train and test predictions and calculate metrics
                train_preds <- f_predict(model, piped_train)
                train_metrics_calculated <- purrr::map_dbl(.x = metrics, function(f) f(unlist(piped_train[response]), train_preds))
                test_preds <- f_predict(model, piped_test)
                test_metrics_calculated <- purrr::map_dbl(.x = metrics, function(f) f(unlist(piped_test[response]), test_preds))

                tmp <- list(".train" = list(f_train), ".predict" = list(f_predict), ".id" = paste0(pipe_name, "_", model_name), "params" = list(parameter_grid[r,]), ".preprocess_pipe" = list(pipe))
                tmp[paste0("train_", metric_names)] <- train_metrics_calculated
                tmp[paste0("test_", metric_names)] <- test_metrics_calculated
                res <- bind_rows(res, tmp)
            }
        }
    }

    return(res)
}

#' Select top models from the find_model function
#'
#' @param train The training dataset
#' @param find_model_result Result from the find_model function
#' @param metric Target metric, as a string
#' @param higher_is_better Logical indicating if the results should be sorted from high to low.
#' @param per_model Logical indicating if we should take N models per model type or in total
#' @param top_n The top n models to return
#' @param aggregate_func Aggregation function to apply. Useful if you choose more than 1 model. Set this to NA to skip it.
#'
#' @return A list of the selected trained models and a predict function.
#' @export
find_best_models <- function(train, find_model_result, metric, higher_is_better, per_model = F, top_n = 1, aggregate_func = NA) {
    stopifnot(
        is.logical(per_model),
        is.logical(higher_is_better),
        is.data.frame(find_model_result),
        is.data.frame(train),
        is.character(metric), length(metric) == 1,
        !any(!c(".id", ".train", metric, "params") %in% colnames(find_model_result)),
        is.function(aggregate_func) || is.na(aggregate_func)
    )
    find_model_result <- find_model_result[order(unlist(find_model_result[,metric]), decreasing = higher_is_better),]

    if(per_model) find_model_result %<>% group_by(.id)
    find_model_result %<>% filter(row_number() <= top_n) %>%
        mutate(model_name = paste0(.id, "_", row_number()))

    models <- apply(X = find_model_result, MARGIN = 1, function(r){
        d <- r$.preprocess_pipe(train)
        m <- do.call(what = r$.train, args = list(data = d) %>% c(unlist(r$params)))
        return(m)
    }) %>% as.list
    names(models) <- find_model_result$model_name

    .predict <- function(data) {
        purrr::pmap_df(list(model = models, pred_func = find_model_result$.predict, pipe_func = find_model_result$.preprocess_pipe), function(model, pred_func, pipe_func) pred_func(model, pipe_func(data)))
    }

    if(is.function(aggregate_func)) .predict_function <- function(data)
        dplyr::as_data_frame(apply(X = .predict(data), MARGIN = 1, FUN = aggregate_func))
    else .predict_function <- .predict
    return(list("models" = models, ".predict" = .predict_function))
}

#' Wrapper function for model inputs to \code{find_model}
#'
#' @param .train The training function. This function has to take a \code{data} argument that stores the training data and a \code{...} argument for the parameters.
#' @param .predict The prediction function. This function needs to take two arguments, where the first is the model and the second the new dataset.
#' @param ... Other parameters to be passed to the training function.
#'
#' @return A list for use as a model input with \code{\link{find_model}}
#' @export
#'
#' @examples
#' d <- data.frame(x = 1:10, y = 1:10 + rnorm(10, sd = .3))
#' model_trainer(
#'     .train = function(data, ...) lm(data = data, formula = y ~ ., ...),
#'     .predict = function(x,y) predict.lm(x,y),
#'     x = FALSE
#' )
model_trainer <- function(.train, .predict, ...) {
    stopifnot(
        is.function(.train), !any(!c("data", "...") %in% formalArgs(.train)),
        is.function(.predict), length(formalArgs(.predict)) >= 2
    )

    return(list(
        .train = .train,
        .predict = .predict,
        ...
    ))
}

#' A convenient wrapper function for find_model for models that have a formula and data argument, like lm
#'
#' @param response The response column, as a string.
#' @param training_function The function that trains the algorithm, such as \code{\link{lm}}
#' @param ... Named vectors of parameters to explore.
#'
#' @return A list that's compatible with the \code{\link{find_model}} function
#' @export
#'
#' @examples
#' # uses svm function from library e1071
#' find_template_formula_and_data(response = "response", training_function = svm,
#'     type = c("eps-regression", "nu-regression"),
#'     kernel = c("radial"), gamma = c(.001, .002, .003, .0008))
find_template_formula_and_data <- function(response, training_function, ...) {
    form <- paste(response, "~ .") %>% as.formula
    return(model_trainer(
        .train = function(data, ...) training_function(formula = form, data = data, ...),
        .predict = function(x, y) {
            if(response %in% colnames(y)) y <- select_(y, paste0("-", response))
            predict(x, y)
        },
        ...
    ))
}

#' A convenient wrapper function for find_model for xgboost
#'
#' @param response The response column, as a string.
#' @param ... Named vectors of parameters to explore.
#'
#' @return A list that's compatible with the \code{\link{find_model}} function
#' @export
#'
#' @importFrom xgboost xgb.train xgb.DMatrix
#' @examples
#' find_xgb(response = "response", nrounds = c(30, 50), eta = c(.015),
#'          max_depth = c(2), colsample_bytree = c(.7), lambda = c(.1),
#'          subsample = c(.7), base_score = c(100))
find_xgb <- function(response, ...) {
    return(model_trainer(
        .train = function(data, ...) xgboost::xgb.train(data = xgboost::xgb.DMatrix(as.matrix(select_(data, paste0("-", response))), label = unlist(data[response])), ...),
        .predict = function(x, y) {
            if(response %in% colnames(y)) y <- select_(y, paste0("-", response))
            predict(x, as.matrix(y))
        },
        ...
    ))
}

#' Expands the params column of the result of \code{find_model}
#'
#' @param find_model_result The result from \code{find_model}
#'
#' @return A dataframe with the params column expanded
#' @export
find_expand_results <- function(find_model_result) {
    return(bind_cols(find_model_result[names(find_model_result) != "params"], bind_rows(find_model_result$params)))
}
