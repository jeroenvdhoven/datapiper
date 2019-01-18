#' Applies different transformations to each numeric feature and selects the one with the highest correlation to the response per feature
#'
#' @param train Data frame containing the train data.
#' @param response String denoting the name of the column that should be used as the response variable. Mandatory
#' @param transform_columns Columns to consider for transformation.
#' @param missing_func A function that determines if an observation is missing, e.g. \code{is.na}, \code{is.nan}, or \code{function(x) x == 0}.
#' Defaults to \code{is.na}. Set to NA to skip NA feature generation and imputation.
#' @param transform_functions A list of function used to transform features. Should be one-to-one transformations like sqrt or log.
#' @param retransform_columns Columns that should be retransformed later on. If this is set to one or more column names, this function will generate a
#' numerical approximation of the inverse of the optimal tranformation function. This pipe will be returned as a separate list entry.
#'
#' @return A list containing the transformed train dataset and a trained pipe. If \code{retransform_columns} was set, the reverting pipe will also be provided.
#' @export
#' @importFrom stats cor
pipe_feature_transformer <- function(train, response, transform_columns, missing_func = is.na,
                                     transform_functions = list(sqrt, log, function(x)x^2), retransform_columns){
    stopifnot(
        response %in% colnames(train), is.numeric(unlist(train[response])),
        is.data.frame(train),
        is.list(transform_functions), !any(!purrr::map_lgl(transform_functions, is.function))
    )
    if(missing(transform_columns)) {
        transform_columns <- colnames(train)[purrr::map_lgl(train, is.numeric)] %>% .[. != response]
    }else stopifnot(!any(!(transform_columns %in% colnames(train))))

    retransform_requested <- !missing(retransform_columns)
    if(retransform_requested) {
        stopifnot(
            is.character(retransform_columns),
            !any(!retransform_columns %in% transform_columns)
        )
        retransform_columns <- transform_columns[transform_columns %in% retransform_columns]
    }

    func_list <- list()
    lower_thresholds <- numeric(0)
    upper_thresholds <- numeric(0)
    for(col in transform_columns){
        res <- feature_transform_col(train, response, transform_functions, col, missing_func)
        best_function <- res$best_function

        if(retransform_requested && (col %in% retransform_columns)) {
            column_values <- unlist(train[col])
            col_min <- min(column_values, na.rm = T)
            col_max <- max(column_values, na.rm = T)

            range_fraction <- .2
            extend_range <- abs(range_fraction * (col_max - col_min))

            lower_threshold <- col_min - extend_range
            upper_threshold <- col_max + extend_range

            # Ensure our threshold is feasible
            suppressWarnings(if(is.nan(best_function(lower_threshold)) || is.na(best_function(lower_threshold))) lower_threshold <- col_min * ifelse(col_min > 0, 1 - range_fraction, 1 + range_fraction))
            suppressWarnings(if(is.nan(best_function(upper_threshold)) || is.na(best_function(upper_threshold))) upper_threshold <- col_max * ifelse(col_max > 0, 1 + range_fraction, 1 - range_fraction))

            lower_thresholds <- c(lower_thresholds, lower_threshold)
            upper_thresholds <- c(upper_thresholds, upper_threshold)
        }

        train <- res$train
        func_list <- c(func_list, best_function)
    }
    names(func_list) <- transform_columns
    predict_pipe <- pipe(.function = feature_transformer_predict, transform_columns = transform_columns, transform_functions = func_list)
    result <- list("train" = train, "pipe" = predict_pipe)

    if(retransform_requested){
        retransform_functions <- func_list[retransform_columns]
        names(lower_thresholds) <- retransform_columns
        names(upper_thresholds) <- retransform_columns

        result$post_pipe <- pipe(.function = feature_transformer_post_predict, retransform_columns = retransform_columns,
                                 lower_thresholds = lower_thresholds, upper_thresholds = upper_thresholds,
                                 retransform_functions = retransform_functions)
    }
    return(result)
}

feature_transform_col <- function(train, response, transform_functions, transform_column, missing_func = is.na){
    vec_train <- unlist(train[, transform_column])
    missing_vector <- missing_func(vec_train)
    vec_train %<>% .[!missing_vector]
    response <- unlist(train[response])[!missing_vector]
    #Include option to remove na's from vec_train

    new_features <- suppressWarnings(purrr::map(transform_functions, function(x)
        vec_train %>% x %>% unlist %>% as.vector
    ))
    any_faulty <- purrr::map_lgl(new_features, function(x) any(is.null(x)) || anyNA(x) || any(is.nan(x)) || any(is.infinite(x)))

    new_features %<>% .[!any_faulty]
    transform_functions %<>% .[!any_faulty]

    if(length(new_features) > 0){
        correlations <- abs(purrr::map_dbl(new_features, . %>% .[seq_len(nrow(train))] %>% .[!missing_vector] %>% cor(y = response, use = "na.or.complete")))
        original_correlation <- cor(response, vec_train, use = "na.or.complete")
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
#' @return Returns the dataset with transformed columns
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

#' Uses the results of \code{\link{pipe_feature_transformer}} on new datasets
#'
#' @param data A new dataset
#' @param retransform_columns The same columns used to obtain these selected retransform_functions
#' @param lower_thresholds A vector of lower treshold used by \code{\link{uniroot}}, used to approximate the inverse function.
#' @param upper_thresholds A vector of upper treshold used by \code{\link{uniroot}}, used to approximate the inverse function.
#' @param retransform_functions Result of \code{\link{pipe_feature_transformer}}
#'
#' @return Returns the dataset with retransformed columns
#' @importFrom stats uniroot
feature_transformer_post_predict <- function(data, retransform_columns, lower_thresholds, upper_thresholds, retransform_functions){
    inverse = function (f, lower = 0, upper = Inf) {
        function (y)  {
            new_f <- function (x) (f(x) - y)
            f_new_lower <- new_f(lower)
            f_new_upper <- new_f(upper)
            f.lower <- ifelse(is.na(f_new_lower) || is.nan(f_new_lower), lower, f_new_lower)
            f.upper <- ifelse(is.na(f_new_upper) || is.nan(f_new_upper), lower, f_new_upper)
            uniroot(f = new_f, lower = lower, upper = upper, tol = sqrt(.Machine$double.eps), maxiter = 100,
                    f.lower = f.lower, f.upper = f.upper)$root
        }
    }

    stopifnot(
        is.data.frame(data),
        is.character(retransform_columns), !any(!retransform_columns %in% colnames(data)),
        is.numeric(lower_thresholds), length(lower_thresholds) == length(retransform_columns),
        is.numeric(upper_thresholds), length(upper_thresholds) == length(retransform_columns),
        is.list(retransform_functions), !any(!purrr::map_lgl(retransform_functions, is.function)),
        length(retransform_functions) == length(retransform_columns)
    )

    for(i in seq_along(retransform_columns)){
        col <- retransform_columns[i]
        inverse_function <- inverse(retransform_functions[[i]], lower = lower_thresholds[i], upper = upper_thresholds[i])
        current_col <- unlist(data[, col])
        current_col[!is.na(current_col)] <- purrr::map_dbl(.x = current_col[!is.na(current_col)], .f = function(x) {
            success <- F
            result <- tryCatch({
                # Will return NA if the inverse function fails.
                var <- inverse_function(x)
                success <- T
                return(var)
            }, error = function(e) {
                warning(as.character(e))
            })
            if(success) return(result)
            else return(NA)
        })
        data[, col] <- current_col
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
#' @param retransform_columns Columns that should be rescaled later on. A new pipe will be created and returned as a separate list entry.
#'
#' @return A list containing the transformed train dataset and a trained pipe. If \code{retransform_columns} was set, the reverting pipe will also be provided.
#' @export
pipe_scaler <- function(train, exclude_columns = character(length = 0), type = "[0-1]", retransform_columns){
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
            !any(!columns %in% colnames(data)),
            is.numeric(centers), length(centers) == length(columns),
            is.numeric(scales), length(scales) == length(columns)
        )

        data[, columns] %<>% scale(center = centers, scale = scales)
        return(data)
    }
    predict_pipe <- pipe(.function = predict_function, centers = centers, scales = scales, columns = columns)
    result <- list("train" = train, "pipe" = predict_pipe)

    retransform_requested <- !missing(retransform_columns)
    if(retransform_requested) {
        stopifnot(
            is.character(retransform_columns),
            !any(!retransform_columns %in% columns)
        )
        retransform_columns <- columns[columns %in% retransform_columns]
        retransform_centers <- centers[columns %in% retransform_columns]
        retransform_scales <- scales[columns %in% retransform_columns]

        post_predict_function <- function(data, centers, scales, columns) {
            stopifnot(
                is.data.frame(data),
                !any(!columns %in% colnames(data)),
                is.numeric(centers), length(centers) == length(columns),
                is.numeric(scales), length(scales) == length(columns)
            )

            for(i in seq_along(columns)) {
                current_col <- unlist(data[, columns[i]])
                current_col <- current_col * scales[i] + centers[i]
                data[, columns[i]] <- current_col
            }
            return(data)
        }
        result$post_pipe <- pipe(.function = post_predict_function, centers = retransform_centers, scales = retransform_scales, columns = retransform_columns)
    }

    return(result)
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

        stats_transformer <- list(pipe = NA)
        generated_columns <- NA
    } else {
        stats_transformer <- pipe_create_stats(
            train = train, stat_cols = columns, response = response, functions = stat_functions,
            interaction_level = 1, too_few_observations_cutoff = 0)


        one_hot_parameters <- as.list(columns)
        names(one_hot_parameters) <- columns
        train_dt_set <- invoke(stats_transformer$pipe, train)

        generated_columns <- colnames(stats_transformer$train)[!colnames(stats_transformer$train) %in% colnames(train)]
        if(is.data.table(train_dt_set)) train_dt_set <- train_dt_set[, .SD, .SDcols = generated_columns]
        else train_dt_set %<>% select_(.dots = generated_columns)
    }

    if(use_pca){
        pca <- prcomp(x = train_dt_set, center = T, scale. = T, tol = pca_tol, retx = F)
        train_dt_set <- predict(pca, newdata = train_dt_set) %>% as_data_frame
    } else pca <- NA
    train %<>% select_(.dots = paste0("-", columns)) %>% dplyr::bind_cols(train_dt_set)

    predict_pipe <- pipe(.function = feature_one_hot_encode_predict, one_hot_parameters = one_hot_parameters,
                         use_pca = use_pca, pca = pca, stat_transformer = stats_transformer$pipe,
                         generated_columns = generated_columns)
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
feature_one_hot_encode_predict <- function(data, one_hot_parameters, use_pca, pca, stat_transformer, generated_columns){
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
        if(is.data.table(test_dt_set)) test_dt_set <- test_dt_set[, .SD, .SDcols = generated_columns]
        else test_dt_set %<>% select_(.dots = generated_columns)
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

#' Apply PCA to a subset of the columns in a dataset
#'
#' @param train Data frame containing the train data.
#' @param columns Columns to use in the PCA transformation.
#' @param pca_tol The \code{tol} of \code{\link[stats]{prcomp}}
#' @param keep_old_columns Flag indicating if columns used to perform the PCA transformation should be kept.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
pipe_pca <- function(train, columns, pca_tol, keep_old_columns = F) {
    stopifnot(
        is.data.frame(train),
        is.character(columns),
        length(columns) > 0,
        !any(!columns %in% colnames(train)),
        is.numeric(pca_tol),
        pca_tol >= 0, pca_tol <= 1
    )

    pca_set <- train[, columns]
    numeric_cols <- purrr::map_lgl(pca_set, ~ is.numeric(.) || is.logical(.))
    if(any(!numeric_cols)) stop("Some selected columns aren't numeric or logical")

    na_rows <- apply(pca_set, 1, anyNA)
    if(any(na_rows)) {
        pca_set <- pca_set[!na_rows, ]
        if(nrow(pca_set) == 0) stop("No rows were left after checking for NA's")

        warning("Removed ", round(mean(na_rows) * 100, digits = 2), "% of rows due to missing values")
    }

    pca <- prcomp(x = pca_set, center = T, scale. = T, tol = pca_tol, retx = F)

    predict_pipe <- pipe(.function = pca_predict, pca = pca, columns = columns, keep_old_columns = keep_old_columns)
    train <- invoke(predict_pipe, train)

    return(list(train = train, pipe = predict_pipe))
}

pca_predict <- function(data, pca, columns, keep_old_columns) {
    stopifnot(
        is.data.frame(data),
        is.character(columns),
        length(columns) > 0,
        !any(!columns %in% colnames(data)),
        "prcomp" %in% class(pca)
    )

    subset <- data[, columns]
    if(anyNA(subset)) {
        warning("Encountered missing values in data to be used for PCA transformation")
    }

    subset_pca <- predict(pca, subset)
    if(!keep_old_columns) data <- dplyr::select_(data, .dots = paste0("-", columns))
    data <- cbind(data, subset_pca)

    return(data)
}
