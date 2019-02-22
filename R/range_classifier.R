#' Generates features for regression problems through classification
#'
#' Use classification models to classify if the response is larger than a series of given values for regression tasks
#'
#' @param train The train dataset, as a data.frame or data.table.
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
#' @param model Type of model to use. Currently only binomial glm and xgboost are available.
#' @param controls Parameters for the models to use. Leave empty or set to NA to use defaults:
#' \itemize{
#' \item glm: \code{\link[stats]{glm.control}}
#' \item xgboost: see \code{\link[xgboost]{xgb.train}}
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
                                  quantiles = 0, even_spreads = 0, values, model = c("glm", "xgboost")[1], controls){
    env <- environment()
    tryCatch(exclude_columns, error = function(e) env[["exclude_columns"]] <- env[["response"]])
    if(missing(base_definitive_column_name)) {
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
    resp <- unlist(select_cols(train, response))

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

    training_subset <- select_cols(train, included_cols)

    for(i in 1:length(values)) {
        value <- values[i]
        temporary_column_name <- paste0(base_temporary_column_name, "_", value)
        definitive_column_name <- paste0(base_definitive_column_name, "_", value)

        if(model == "glm"){
            if(is.data.table(training_subset)) training_subset[, c(temporary_column_name) := resp > value]
            else training_subset[temporary_column_name] <- resp > value
            form <- paste0("`", temporary_column_name, "` ~ `", paste0(included_cols, collapse = '` + `'), "`") %>% as.formula

            if(missing(controls) || is.na(controls)) controls <- glm.control()
            range_model <- glm(formula = form, data = training_subset, control = controls, family = "binomial")
            train_pred <- predict(range_model, newdata = training_subset)
            deselect_cols(training_subset, temporary_column_name, inplace = T)
        } else if(model == "xgboost"){
            if(missing(controls) || is.na(controls)) controls <- list(
                objective = "binary:logistic",
                niter = 25
            )
            labels <- resp > value

            xgb_data <- xgboost::xgb.DMatrix(as.matrix(training_subset), label = labels)
            range_model <- xgboost::xgb.train(params = controls, nrounds = controls$niter, data = xgb_data)
            train_pred <- predict(range_model, newdata = xgb_data)
        }

        min_t_p <- min(train_pred);
        max_t_p <- max(train_pred);

        train_pred <- (train_pred - min_t_p) / (max_t_p - min_t_p)

        if(is.data.table(train)) train[, c(definitive_column_name) := train_pred]
        else train[definitive_column_name] <- train_pred

        models[[i]] <- range_model
        scales[[i]] <- c("min" = min_t_p, "max" = max_t_p)
        column_names[i] <- definitive_column_name
    }

    predict_pipe <- pipe(.function = range_predict, models = models, column_names = column_names,
                         scales = scales, included_columns = included_cols)
    return(list(train = train, pipe = predict_pipe))
}

#' Use generated models and scales to create features for a new dataset
#'
#' @param data The new dataset, as a data.frame or data.table.
#' @param models List of models generated by \code{\link{pipe_range_classifier}}
#' @param column_names List of full column names for the newly generated features.
#' @param scales List of scales generated by \code{\link{pipe_range_classifier}}
#' @param included_columns Columns that should be used in the models.
#'
#' @return \code{data}, but including the new features as columns
range_predict <- function(data, models, column_names, scales, included_columns){
    stopifnot(
        !missing(data), is.data.frame(data),
        !missing(models), is.list(models),
        !missing(scales), is.list(scales), !any(purrr::map_dbl(scales, length) != 2),
        is.character(column_names), length(models) == length(column_names),
        !any(column_names %in% colnames(data)),
        !any(!included_columns %in% colnames(data))
    )

    input_data <- select_cols(data, included_columns)

    for(i in seq_along(models)) {
        if("glm" %in% class(models[[i]])) {
            new_column <- predict(models[[i]], input_data)
        } else if("xgb.Booster" %in% class(models[[i]])) {
            new_column <- predict(models[[i]], as.matrix(input_data))
        }
        new_column <- (new_column - scales[[i]][1]) / (scales[[i]][2] - scales[[i]][1])

        if(is.data.table(data)) data[, c(column_names[i]) := new_column]
        else data[column_names[i]] <- new_column

    }
    return(data)
}
