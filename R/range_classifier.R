#' Generates features for regression problems through classification
#'
#' Use classification models to classify if the response is larger than a series of given values for regression tasks
#'
#' @param train Data frame containing the train data.
#' @param response_col String denoting the name of the column that should be used as the response_col variable.
#' @param exclude_columns Columns that shouldn't be used in the models. Defaults to the response column and will ALWAYS include the response column.
#' @param base_temporary_column_name Base name that will be used to create a temporary variable for training the classifier. Use this to ensure no existing columns are overwritten.
#' @param base_definitive_column_name Base name that will be used to store the predictions of the created classifiers. Will be appended by the threshold value.
#' Use this to ensure no existing columns are overwritten.
#' @param quantiles Number of quantiles to use to generate threshold values. Will actually generate \code{quantiles+2} quantiles and look at 2nd to \code{quantiles+1}-th
#' quantiles to remove non-sensical thresholds. Non-negative integer, defaults to 10.
#' @param even_spreads Number of evenly spread thresholds to use. These will be based on the minimum and maximum value of the response_col in \code{train}. Defines its thresholds simarly to \code{quantiles}
#' @param values Threshold values to use. We will check if these fall in the range of the response_col in \code{train}.
#' @param model Type of model to use. Currently only binomial glm is available.
#' @param controls Parameters for the models to use. Leave empty or set to NA to use defaults:
#' \itemize{
#' \item glm: \code{\link[stats]{glm.control}}
#' }
#' @param tolerable_performance_reduction If \code{test} contains the response column, we will test each model if it's good enough.
#' We will calculate the AUC of each model on train and test sets and if the AUC on test is bigger than \code{1-tolerable_performance_reduction} times the model is deemed good enough.
#'
#' @details If multiple values out of \code{quantiles}, \code{even_spreads}, or \code{values} are chosen, all options will be applied.
#'
#' @return A list containing the transformed train dataset, a .predict function to repeat the process on new data and all parameters needed to replicate the process.
#' @importFrom xgboost xgb.train xgb.DMatrix
#' @export
range_classifier <- function(train, response_col, exclude_columns = response_col,
                             tolerable_performance_reduction = .2,
                             base_temporary_column_name = "base_temporary_column_name",
                             base_definitive_column_name = paste0(response_col, "_quantile"),
                             quantiles = 10, even_spreads, values, model = c("glm"), controls){
    env <- environment()
    tryCatch(exclude_columns, error = function(e) env[["exclude_columns"]] <- env[["response_col"]])
    tryCatch(base_definitive_column_name, error = function(e) env[["base_definitive_column_name"]] <- env[[paste0(response_col, "_quantile")]])
    stopifnot(
        !missing(train), is.data.frame(train),
        is.character(response_col), length(response_col) == 1, response_col %in% colnames(train),
        is.character(exclude_columns), !any(!exclude_columns %in% colnames(train)),
        is.character(base_temporary_column_name), length(base_temporary_column_name) == 1,
        !any(grepl(pattern = base_temporary_column_name, colnames(train))),
        tolerable_performance_reduction >= 0, tolerable_performance_reduction <= 1
    )
    if(!response_col %in% exclude_columns) exclude_columns %<>% c(response_col)
    resp <- unlist(train[response_col])

    if(missing(values) || is.na(values)) values <- numeric(length = 0)

    if(!missing(quantiles) && !anyNA(quantiles)){
        quantile_values <- quantile(x = resp, probs = seq(0, 1, length.out = quantiles + 2))[2:(quantiles + 1)]
        values <- c(values, quantile_values)
    }

    if(!missing(even_spreads) && !anyNA(even_spreads)){
        even_values <- seq(min(resp), max(resp), length.out = even_spreads + 2)[2:(even_spreads + 1)]
        values <- c(values, even_values)
    }

    values <- sort(values, decreasing = F)

    # if(missing(values) && missing(even_spreads)) {
    #     stopifnot(is.numeric(quantiles), quantiles > 0)
    #     values <- quantile(x = resp, probs = seq(0, 1, length.out = quantiles + 2))[2:(quantiles + 1)]
    # }else if(!missing(values)){
    #     #Ensure values are numeric and in the range {max(resp) >= values >= min(resp)}
    #     stopifnot(is.numeric(values), !any(values > max(resp)), !any(values < min(resp)))
    # }else if(!missing(even_spreads)){
    #     stopifnot(is.numeric(even_spreads), even_spreads > 0)
    #     values <- seq(min(resp), max(resp), length.out = even_spreads + 2)[2:(even_spreads + 1)]
    # }

    included_cols <- colnames(train) %>% .[
        (!. %in% exclude_columns ) &          #All columns that are not excluded.
            (!grepl(pattern = response_col, x = ., fixed = T)) #All columns that contain exactly the same column name.
        ]

    conserved_models <- rep(T, length(values))
    models <- as.list(1:length(values))
    scales <- as.list(1:length(values))
    column_names <- character(length = length(values))

    # test_contains_response <- response_col %in% colnames(test)
    # if(test_contains_response) {
    #     resp_test <- unlist(test[response_col])
    #     aucs <- data_frame(
    #         values = values,
    #         train = rep(0, length(values)),
    #         test = rep(0, length(values))
    #     )
    # }

    for(i in 1:length(values)) {
        value <- values[i]
        temporary_column_name <- paste0(base_temporary_column_name, "_", value)
        definitive_column_name <- paste0(base_definitive_column_name, "_", value)
        train[temporary_column_name] <- resp > value
        form <- paste0("`", temporary_column_name, "` ~ `", paste0(included_cols, collapse = '` + `'), "`") %>% as.formula

        if(model == "glm"){
            if(missing(controls) || is.na(controls)) controls <- glm.control()
            range_model <- glm(formula = form, data = train, control = controls, family = "binomial")
            train_pred <- predict(range_model, newdata = train)
            # test_pred <- predict(range_model, newdata = test)
        } else if(model == "xgboost"){
            if(missing(controls) || is.na(controls)) controls <- list(
                objective = "binary:logistic",
                niter = 25
            )
            xgb_data <- xgboost::xgb.DMatrix(as.matrix(train[included_cols]), label = unlist(train[temporary_column_name]))
            range_model <- xgboost::xgb.train(params = controls, nrounds = controls$niter, data = xgb_data)
            train_pred <- predict(range_model, newdata = as.matrix(train[included_cols]))
            # test_pred <- predict(range_model, newdata = as.matrix(test))
        }

        min_t_p <- min(train_pred);
        max_t_p <- max(train_pred);

        train_pred <- (train_pred - min_t_p) / (max_t_p - min_t_p)
        # test_pred <- (test_pred - min_t_p) / (max_t_p - min_t_p)

        conserve_model <- T
        # Validate if model is somewhat good.
        # if(test_contains_response){
        #     auc_train <- util_auc_model(labels = unlist(train[temporary_column_name]), predicted = train_pred, plotting = F)
        #     auc_test <- util_auc_model(labels = resp_test > value, predicted = test_pred, plotting = F)
        #
        #     if(auc_train * (1 - tolerable_performance_reduction) > auc_test)  conserve_model <- F
        #     aucs$train[i] <- auc_train
        #     aucs$test[i] <- auc_test
        # }
        conserved_models[i] <- conserve_model
        train[temporary_column_name] <- NULL

        # Perhaps define cutoff and generate mean / stdev per group.
        if(conserve_model){
            train[definitive_column_name] <- train_pred
            # test[definitive_column_name] <- test_pred

            models[[i]] <- range_model
            scales[[i]] <- c("min" = min_t_p, "max" = max_t_p)
            column_names[i] <- definitive_column_name
        }
    }

    models <- models[conserved_models]
    scales <- scales[conserved_models]
    column_names <- column_names[conserved_models]

    predict_function <- function(data) range_predict(data = data, models = models, column_names = column_names,
                                                     scales = scales, excluded_columns = exclude_columns)
    res <- list(
        train = train,
        models = models,
        scales = scales,
        column_names = column_names,
        .predict = predict_function
    )
    # if(test_contains_response) res$aucs <- aucs
    return(res)
}

#' Use generated models and scales to create features for a new dataset
#'
#' @param data Dataset for new features
#' @param models List of models generated by \code{\link{range_classifier}}
#' @param column_names List of full column names for the newly generated features.
#' @param scales List of scales generated by \code{\link{range_classifier}}
#' @param excluded_columns Columns that shouldn't be used in the models.
#'
#' @return \code{data}, but including the new features as columns
#' @export
range_predict <- function(data, models, column_names, scales, excluded_columns){
    stopifnot(
        !missing(data), is.data.frame(data),
        !missing(models), is.list(models),
        !missing(scales), is.list(scales), !any(purrr::map_dbl(scales, length) != 2),
        is.character(column_names), length(models) == length(column_names),
        !any(column_names %in% colnames(data)),
        !any(!excluded_columns %in% colnames(data))
    )
    included_columns <- colnames(data)[!colnames(data) %in% excluded_columns]

    for(i in seq_along(models)) {
        if("glm" %in% class(models[[i]]))
            data[column_names[i]] <- (predict(models[[i]], data[, included_columns]) - scales[[i]][1]) / (scales[[i]][2] - scales[[i]][1])
        else if("xgb.Booster" %in% class(models[[i]]))
            data[column_names[i]] <- (predict(models[[i]], as.matrix(data[, included_columns])) - scales[[i]][1]) / (scales[[i]][2] - scales[[i]][1])
    }
    return(data)
}
