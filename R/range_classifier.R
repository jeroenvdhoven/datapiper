#' Generates features for regression problems through classification
#'
#' Use classification models to classify if the response is larger than a series of given values for regression tasks
#'
#' @param train Data frame containing the train data.
#' @param response String denoting the name of the column that should be used as the response variable.
#' @param exclude_columns Columns that shouldn't be used in the models. Defaults to the response column and will ALWAYS include the response column.
#' @param base_temporary_column_name Base name that will be used to create a temporary variable for training the classifier. Use this to ensure no existing columns are overwritten.
#' @param base_definitive_column_name Base name that will be used to store the predictions of the created classifiers. Will be appended by the threshold value.
#' Use this to ensure no existing columns are overwritten.
#' @param quantiles Number of quantiles to use to generate threshold values. Will actually generate \code{quantiles+2} quantiles and look at 2nd to \code{quantiles+1}-th
#' quantiles to remove non-sensical thresholds. Non-negative integer, defaults to 0.
#' @param even_spreads Number of evenly spread thresholds to use. These will be based on the minimum and maximum value of the response in \code{train}. Defines its thresholds simarly to \code{quantiles}
#' Non-negative integer, defaults to 0.
#' @param values Threshold values to use. We will check if these fall in the range of the response in \code{train}.
#' @param model Type of model to use. Currently only binomial glm is available.
#' @param controls Parameters for the models to use. Leave empty or set to NA to use defaults:
#' \itemize{
#' \item glm: \code{\link[stats]{glm.control}}
#' }
#'
#' @details If multiple values out of \code{quantiles}, \code{even_spreads}, or \code{values} are chosen, all options will be applied.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @importFrom xgboost xgb.train xgb.DMatrix
#' @importFrom stats glm predict as.formula quantile glm.control
#' @export
pipe_range_classifier <- function(train, response, exclude_columns = response,
                                  base_temporary_column_name = "base_temporary_column_name",
                                  base_definitive_column_name = paste0(response, "_quantile"),
                                  quantiles = 0, even_spreads = 0, values, model = c("glm"), controls){
    env <- environment()
    tryCatch(exclude_columns, error = function(e) env[["exclude_columns"]] <- env[["response"]])
    if(!missing(base_definitive_column_name)) {
        base_definitive_column_name <- paste0(response, "_quantile")
    }
    stopifnot(
        !missing(train), is.data.frame(train),
        is.character(response), length(response) == 1, response %in% colnames(train),
        is.character(exclude_columns), !any(!exclude_columns %in% colnames(train)),
        is.character(base_temporary_column_name), length(base_temporary_column_name) == 1,
        !any(grepl(pattern = base_temporary_column_name, colnames(train)))
    )
    if(!response %in% exclude_columns) exclude_columns %<>% c(response)
    resp <- unlist(train[response])

    if(missing(values) || anyNA(values)) values <- numeric(length = 0)

    if(!missing(quantiles) && !is.na(quantiles) && quantiles > 0){
        stopifnot(is.numeric(quantiles), length(quantiles) == 1)
        quantile_values <- quantile(x = resp, probs = seq(0, 1, length.out = quantiles + 2), na.rm = T)[2:(quantiles + 1)]
        values <- c(values, quantile_values)
    }

    if(!missing(even_spreads) && !is.na(even_spreads) && even_spreads > 0){
        stopifnot(is.numeric(even_spreads), length(even_spreads) == 1)
        even_values <- seq(min(resp, na.rm = T), max(resp, na.rm = T), length.out = even_spreads + 2)[2:(even_spreads + 1)]
        values <- c(values, even_values)
    }

    values <- sort(values, decreasing = F)

    included_cols <- colnames(train) %>% .[
        (!. %in% exclude_columns ) &          #All columns that are not excluded.
            (!grepl(pattern = response, x = ., fixed = T)) #All columns that contain exactly the same column name.
        ]

    models <- as.list(1:length(values))
    scales <- as.list(1:length(values))
    column_names <- character(length = length(values))

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
        } else if(model == "xgboost"){
            if(missing(controls) || is.na(controls)) controls <- list(
                objective = "binary:logistic",
                niter = 25
            )
            xgb_data <- xgboost::xgb.DMatrix(as.matrix(train[included_cols]), label = unlist(train[temporary_column_name]))
            range_model <- xgboost::xgb.train(params = controls, nrounds = controls$niter, data = xgb_data)
            train_pred <- predict(range_model, newdata = as.matrix(train[included_cols]))
        }

        min_t_p <- min(train_pred);
        max_t_p <- max(train_pred);

        train_pred <- (train_pred - min_t_p) / (max_t_p - min_t_p)

        train[temporary_column_name] <- NULL
        train[definitive_column_name] <- train_pred

        models[[i]] <- range_model
        scales[[i]] <- c("min" = min_t_p, "max" = max_t_p)
        column_names[i] <- definitive_column_name
    }

    predict_pipe <- pipe(.function = range_predict, models = models, column_names = column_names,
                         scales = scales, excluded_columns = exclude_columns)
    return(list(train = train, pipe = predict_pipe))
}

#' Use generated models and scales to create features for a new dataset
#'
#' @param data Dataset for new features
#' @param models List of models generated by \code{\link{pipe_range_classifier}}
#' @param column_names List of full column names for the newly generated features.
#' @param scales List of scales generated by \code{\link{pipe_range_classifier}}
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
