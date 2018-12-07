#' Applies different transformations to each numeric feature and selects the one with the highest correlation to the response per feature
#'
#' @param train Data frame containing the train data.
#' @param response String denoting the name of the column that should be used as the response variable. Mandatory
#' @param transform_columns Columns to consider for transformation.
#' @param missing_func A function that determines if an observation is missing, e.g. \code{is.na}, \code{is.nan}, or \code{function(x) x == 0}. Defaults to \code{is.na}. Set to NA to skip NA feature generation and imputation.
#' @param transform_functions A list of function used to transform features. Should be one-to-one transformations like sqrt or log.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
#' @importFrom stats lm cor
pipe_feature_transformer <- function(train, response, transform_columns, missing_func = is.na,
                                     transform_functions = list(sqrt, log, function(x)x^2)){
    stopifnot(
        response %in% colnames(train), is.numeric(unlist(train[response])),
        is.data.frame(train),
        is.list(transform_functions), !any(!purrr::map_lgl(transform_functions, is.function))
    )
    if(missing(transform_columns)) {
        transform_columns <- colnames(train)[purrr::map_lgl(train, is.numeric)] %>% .[. != response]
    }else stopifnot(!any(!(transform_columns %in% colnames(train))))

    func_list <- list()
    for(col in transform_columns){
        res <- feature_transform_col(train, response, transform_functions, col, missing_func, comparison_method = "cor")
        train <- res$train
        func_list %<>% c(res$best_function)
    }

    predict_pipe <- pipe(.function = feature_transformer_predict, transform_columns = transform_columns, transform_functions = func_list)
    return(list("train" = train, "pipe" = predict_pipe))
}

feature_transform_col <- function(train, response, transform_functions, transform_column, missing_func = is.na, comparison_method){
    vec_train <- unlist(train[transform_column])
    missing_vector <- missing_func(vec_train)
    vec_train %<>% .[!missing_vector]
    response <- unlist(train[response])[!missing_vector]
    #Include option to remove na's from vec_train

    tot_vec <- unlist(train[transform_column]) %>% .[!missing_func(.)]
    new_features <- suppressWarnings(purrr::map(transform_functions, function(x)
        tot_vec %>% x %>% unlist %>% as.vector
    ))
    any_faulty <- purrr::map_lgl(new_features, function(x) any(is.null(x)) || anyNA(x) || any(is.nan(x)) || any(is.infinite(x)))

    new_features %<>% .[!any_faulty]
    transform_functions %<>% .[!any_faulty]

    if(length(new_features) > 0){
        if(comparison_method == "cor"){
            correlations <- abs(purrr::map_dbl(new_features, . %>% .[1:nrow(train)] %>% .[!missing_vector] %>% cor(y = response, use = "na.or.complete")))
            original_correlation <- cor(response, vec_train, use = "na.or.complete")
        } else if (comparison_method == "lm") {
            test_dfs <- purrr::map(new_features, . %>% .[1:nrow(train)] %>% .[!missing_vector] %>% data_frame(x = ., y = response))
            correlations <- purrr::map_dbl(test_dfs, function(x) summary(lm(formula = y ~ x, data = x))$adj.r.squared)
            original_correlation <- cor(response, vec_train, use = "na.or.complete")
        }

        if(max(correlations) > original_correlation){
            best_function <- transform_functions[correlations == max(correlations)][[1]]
            train[transform_column] %<>% best_function %>% unlist %>% as.vector
        }else best_function <- function(x) x
    }else best_function <- function(x) x
    res <- list("train" = train, "best_function" = best_function)
    return(res)
}

#' Uses the results of \code{\link{pipe_feature_transformer}} on new datasets
#'
#' @param data A new dataset
#' @param transform_columns The same columns used to obtain these selected transform_functions
#' @param transform_functions Result of \code{\link{pipe_feature_transformer}}
#'
#' @return Returns
feature_transformer_predict <- function(data, transform_columns, transform_functions){
    #test input
    stopifnot(
        is.data.frame(data),
        is.character(transform_columns), !any(!transform_columns %in% colnames(data)),
        is.list(transform_functions), !any(!purrr::map_lgl(transform_functions, is.function))
    )

    for(i in seq_along(transform_columns)){
        col <- transform_columns[i]
        data[, col] %<>% unlist %>% {transform_functions[[i]](.)}
    }

    return(data)
}

#' Rescales data to standardised ranges
#'
#' @param train Data frame containing the train data.
#' @param exclude_columns Names of columns that should be excluded from rescaling
#' @param type Type of rescales to perform:
#' \itemize{
#' \item \code{"[0-1]"}: rescales the columns to the [0-1] range
#' \item \code{"N(0,1)"}: rescales the columns to mean 0 and sd 1
#' }
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
pipe_scaler <- function(train, exclude_columns = character(length = 0), type = "[0-1]"){
    stopifnot(
        is.data.frame(train),
        nrow(train) > 0,
        is.character(exclude_columns),
        !any(!exclude_columns %in% colnames(train)),
        type %in% c("[0-1]", "N(0,1)")
    )

    # Ensure non-numeric columns are never standardised
    exclude_columns <- unique(c(exclude_columns, colnames(train)[!purrr::map_lgl(train, is.numeric)]))
    columns <- colnames(train)[!colnames(train) %in% exclude_columns]

    if(type == "N(0,1)") {
        center_func <- function(x) mean(x, na.rm = T)
        scale_func <- function(x) sd(x, na.rm = T)
    } else if(type == "[0-1]"){
        center_func <- function(x) min(x, na.rm = T)
        scale_func <- function(x) max(x, na.rm = T) - min(x, na.rm = T)
    }else stop("Error: wrong type of scaling")

    centers <- purrr::map_dbl(train[, columns], center_func)
    scales <- purrr::map_dbl(train[, columns], scale_func)
    scales[scales == 0] <- 1L

    train[, columns] %<>% scale(center = centers, scale = scales)

    predict_function <- function(data, centers, scales, columns) {
        stopifnot(
            is.data.frame(data),
            !any(!columns %in% colnames(data))
        )

        data[, columns] %<>% scale(center = centers, scale = scales)
        return(data)
    }

    predict_pipe <- pipe(.function = predict_function, centers = centers, scales = scales, columns = columns)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Train one-hot encoding
#'
#' @param train Dataframe to determine one-hot-encoding.
#' @param columns Columns from \code{train} to use for one-hot-encoding. Will automatically check if theses are column names in \code{train}
#' @param use_pca Whether PCA transformation is required.
#' @param pca_tol The \code{tol} of \code{\link[stats]{prcomp}}
#' @param stat_functions A (named) list of functions for when you want to use mean-encoding. Don't set it if you want to do regular one-hot encoding.
#' Any function that return a single value from a scalar would do (e.g. quantile, sd).
#' @param response String denoting the name of the column that should be used as the response variable. Mandatory
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
#' @importFrom data.table .SD :=
#' @importFrom stats prcomp
pipe_one_hot_encode <- function(train, columns = colnames(train)[purrr::map_lgl(train, function(x) return(!(is.numeric(x) || is.logical(x))))],
                                stat_functions, response,
                                use_pca = FALSE, pca_tol = .1){
    stopifnot(
        is.data.frame(train),
        is.character(columns),
        columns %in% colnames(train) %>% not %>% any %>% not,
        missing(stat_functions) ||
            (is.list(stat_functions) &&
                 !any(is.null(names(stat_functions))) &&
                 !any(!purrr::map_lgl(stat_functions, is.function)) &&
                 is.character(response) &&
                 response %in% colnames(train)
            ),
        is.logical(use_pca),
        is.numeric(pca_tol),
        pca_tol <= 1, pca_tol > 0
    )

    if(missing(stat_functions)){
        train_dt_set <- train %>% data.table::as.data.table() %>% .[, columns, with = F]

        one_hot_parameters <- list()
        for(colname in columns){
            unique_values <- unique(train_dt_set[, colname, with = F]) %>% unlist
            one_hot_parameters[[colname]] <- unique_values
            train_dt_set[, (paste0(colname, '_', unique_values)) := purrr::map(unique_values, function(x) {
                col <- get(colname)
                as.integer(!is.na(col) & col == x)
            })][, (colname) := NULL]
        }
        delete_cols <- colnames(train_dt_set) %>% .[grepl(x = ., pattern = "_(<NA>)|(NA)$")]
        if(length(delete_cols) > 0) train_dt_set[, (delete_cols) := NULL]

        stats_transformer <- NA
        stats_transformer_pipe <- NA
    } else {
        stats_transformer <- pipe_create_stats(
            train = train, stat_cols = columns, response = response, functions = stat_functions,
            interaction_level = 1, too_few_observations_cutoff = 0)

        current_columns <- colnames(train)
        stats_transformer_pipe <- pipe(.function = stat_transformer_for_one_hot, stats_transformer = stats_transformer$pipe,
                                       orignal_column = current_columns)

        one_hot_parameters <- as.list(columns)
        names(one_hot_parameters) <- columns
        train_dt_set <- invoke(stats_transformer_pipe, train)
    }

    if(use_pca){
        pca <- prcomp(x = train_dt_set, center = T, scale. = T, tol = pca_tol, retx = F)
        train_dt_set <- predict(pca, newdata = train_dt_set) %>% as_data_frame
    } else pca <- NA
    train %<>% select_(.dots = paste0("-", columns)) %>% dplyr::bind_cols(train_dt_set)

    predict_pipe <- pipe(.function = feature_one_hot_encode_predict, one_hot_parameters = one_hot_parameters,
                         use_pca = use_pca, pca = pca, stat_transformer = stats_transformer_pipe)
    return(list(train = train, pipe = predict_pipe))
}

#' Apply one-hot encoding
#'
#' @param data A new dataframe
#' @param one_hot_parameters The \code{one_hot_parameters} element from the result of \code{\link{pipe_one_hot_encode}}
#' @param use_pca Whether PCA transformation is required.
#' @param pca The PCA result from \code{\link{pipe_one_hot_encode}}. Required iff \code{use_pca} is \code{TRUE}
#' @param stat_transformer Stat transformer function from \code{\link{pipe_one_hot_encode}}, or NA (for normal one-hot encoding)
#'
#' @return The test dataframe with the required columns one-hot encoded
feature_one_hot_encode_predict <- function(data, one_hot_parameters, use_pca, pca, stat_transformer){
    stopifnot(
        is.data.frame(data),
        is.list(one_hot_parameters),
        !is.null(names(one_hot_parameters)) || is.function(stat_transformer)
    )
    columns <- names(one_hot_parameters)

    stopifnot(columns %in% colnames(data) %>% not %>% any %>% not)

    if(!is.pipe(stat_transformer) && !is.pipeline(stat_transformer)){
        test_dt_set <- data %>% data.table::as.data.table() %>% .[, columns, with = F]
        for(colname in columns){
            unique_values <- one_hot_parameters[colname] %>% unlist
            test_dt_set[, (paste0(colname, '_', unique_values)) := purrr::map(unique_values, function(x) {
                col <- get(colname)
                as.integer(!is.na(col) & col == x)
            })][, (colname) := NULL]
        }

        delete_cols <- colnames(test_dt_set) %>% .[grepl(x = ., pattern = "_(<NA>)|(NA)$")]
        if(length(delete_cols) > 0) test_dt_set[, (delete_cols) := NULL]
    } else {
        test_dt_set <- invoke(stat_transformer, data)
    }

    if(use_pca) {
        test_dt_set <- data.table::as.data.table(test_dt_set)
        row_names <- rownames(pca$rotation)
        present_row_names <- row_names %in% colnames(test_dt_set)

        test_dt_set <- test_dt_set[, row_names[present_row_names], with = F]
        if(any(!present_row_names)) test_dt_set[, (row_names[!present_row_names]) := 0L]

        test_dt_set <- predict(pca, newdata = test_dt_set) %>% as_data_frame
    }

    data %<>% select_(.dots = paste0("-", columns)) %>% dplyr::bind_cols(test_dt_set)
    return(data)
}

stat_transformer_for_one_hot <- function(data, stats_transformer, orignal_column) {
    data <- invoke(stats_transformer, data)
    data <- data[, !colnames(data) %in% orignal_column]
    return(data)
}

#' Remove values from categorical variables that do not occur often enough
#'
#' @param train Data frame containing the train data.
#' @param response The response column, as a string. Will only be used to ensure this is not included in the \code{categorical_columns} variable
#' @param categorical_columns The columns to apply the filter over. Should be a character vector.
#' @param insufficient_occurance_marker The value to substitute when another value in the categorical column doesn't occur often enough.
#' @param threshold_function A function that takes the train dataset and produces a scalar value: the minimum number of times a value has to occur if it should be preserved.
#'
#' @details Be careful: if for instance only one value gets substituted in a column, then the \code{insufficient_occurance_marker} value will just replace that one, preserving the problem.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
pipe_categorical_filter <- function(
    train, response, insufficient_occurance_marker = "insignificant_category",
    categorical_columns = colnames(train)[purrr::map_lgl(train, is.character)],
    threshold_function = function(data) 30
) {
    categorical_columns <- categorical_columns[categorical_columns != response]
    stopifnot(
        is.data.frame(train),
        is.character(categorical_columns),
        !any(!categorical_columns %in% colnames(train))
    )

    mappings <- as.list(seq_along(categorical_columns))
    names(mappings) <- categorical_columns
    threshold <- threshold_function(train)

    for(i in seq_along(categorical_columns)) {
        col <- categorical_columns[i]
        values <- unlist(train[col])
        tabulated <- table(values)

        mapping <- names(tabulated)
        names(mapping) <- names(tabulated)

        mapping[tabulated < threshold] <- insufficient_occurance_marker
        mappings[[i]] <- mapping
    }

    predict_pipe <- pipe(.function = feature_categorical_filter_predict, categorical_columns = categorical_columns,
                         mappings = mappings, insufficient_occurance_marker = insufficient_occurance_marker)
    train <- invoke(predict_pipe, train)

    return(list(train = train, pipe = predict_pipe))
}

#' Filters categorical variables for new datasets
#'
#' @param data A new dataset
#' @param categorical_columns The columns from \code{feature_categorical_filter}
#' @param mappings The mapping from \code{feature_categorical_filter}
#' @param insufficient_occurance_marker The marker from \code{feature_categorical_filter}
#'
#' @return The transformed dataset
feature_categorical_filter_predict <- function(data, categorical_columns, mappings, insufficient_occurance_marker) {
    stopifnot(
        is.data.frame(data),
        is.list(mappings),
        !any(purrr::is_logical(is.character)),
        !any(purrr::is_logical(~ is.null(names(.))))
    )
    if(any(!categorical_columns %in% colnames(data))) {
        missing_cols <- categorical_columns %>% .[!. %in% colnames(data)] %>% paste0(collapse = ", ")
        stop(paste("Error, columns weren't found in your dataset:", missing_cols))
    }

    for(i in seq_along(categorical_columns)) {
        mapping <- mappings[[i]]
        col <- categorical_columns[i]

        values <- unlist(data[col])
        temp_mapping <- mapping[values]
        temp_mapping[!values %in% names(mapping)] <- insufficient_occurance_marker

        data[col] <- temp_mapping
    }

    return(data)
}

