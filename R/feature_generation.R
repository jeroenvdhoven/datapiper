#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param train The train dataset, as a data.frame.
#' @param conditions Vector of functions / values. All functions / values will be tested against each column. If at least one function returns true or one value matches, the records is considered a missing value.
#' @param columns The columns to check for missing values. Can be provided as logicals, integers, or characters
#' @param force_column If true, always add new columns, even if no missing values were found.
#'
#' @return The datasets (in a list) with NA's properly processed.
#' @export
#'
#' @import magrittr
#' @importFrom purrr map
feature_NA_indicators <- function(train, conditions = list(is.na), columns = colnames(train), force_column = F){
    data <- train
    train_pos <- seq_len(nrow(train))

    if(is.logical(columns)) columns %<>% "*"(1L:ncol(data)) %>% .[. != 0]
    else if(is.character(columns)) columns %<>% match(colnames(data)) %>% .[!is.na(.)]
    else stopifnot(is.integer(columns), length(conditions) > 0)

    if(is.function(conditions)) conditions %<>% list
    func_list <- conditions %>% .[purrr::map_lgl(., is.function)]
    value_list <- conditions %>% .[!purrr::map_lgl(., is.function)]

    final_columns <- character(0)
    for(i in columns){
        x = data[,i]

        y_func <- func_list %>% purrr::map(function(f, X) f(X), X = x) %>% as.data.frame
        y_value <- value_list %>% purrr::map(function(v, X) v == X, X = x) %>% as.data.frame

        if(nrow(y_func) == 0) y <- y_value %>% apply(1, any) %>% as.integer
        else if(nrow(y_value) == 0) y <- y_func %>% apply(1, any, na.rm = T) %>% as.integer
        else y <- cbind(y_func, y_value) %>% apply(1, any) %>% as.integer
        if(any(y) || force_column) {
            data[paste(colnames(data)[i],"NA_indicator", sep = "_")] <- y
            final_columns <- c(final_columns, colnames(data)[i])
        }
    }

    train <- data[train_pos,]
    predict_function <- function(data) feature_NA_indicators_predict(data = data, columns = final_columns, conditions = conditions)
    return(list("train" = train, "conditions" = conditions, "columns" = final_columns, .predict = predict_function))
}

#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param data The new dataset, as a data.frame.
#' @param conditions Vector of functions / values. All functions / values will be tested against each column. If at least one function returns true or one value matches, the records is considered a missing value.
#' @param columns The columns to check for missing values. Can be provided as logicals, integers, or characters
#'
#' @return The dataset with NA's properly processed.
#' @export
#'
#' @import magrittr
#' @importFrom purrr map
feature_NA_indicators_predict <- function(data, conditions, columns){
    if(is.logical(columns)) columns %<>% "*"(1L:ncol(data)) %>% .[. != 0]
    else if(is.character(columns)) columns %<>% match(colnames(data)) %>% .[!is.na(.)]
    else stopifnot(is.integer(columns), length(conditions) > 0)

    if(is.function(conditions)) conditions %<>% list
    func_list <- conditions %>% .[purrr::map_lgl(., is.function)]
    value_list <- conditions %>% .[!purrr::map_lgl(., is.function)]

    for(i in columns){
        x = data[,i]

        y_func <- func_list %>% purrr::map(function(f, X) f(X), X = x) %>% as.data.frame
        y_value <- value_list %>% purrr::map(function(v, X) v == X, X = x) %>% as.data.frame

        if(nrow(y_func) == 0) y <- y_value %>% apply(1, any) %>% as.integer
        else if(nrow(y_value) == 0) y <- y_func %>% apply(1, any, na.rm = T) %>% as.integer
        else y <- cbind(y_func, y_value) %>% apply(1, any) %>% as.integer
        data[paste(colnames(data)[i],"NA_indicator", sep = "_")] <- y
    }

    return(data)
}

#' Generic function for creating statistics on the response column, based on custom columns.
#'
#' @param train The train dataset, as a data.frame. Will generate the statistics only based on this dataset.
#' @param stat_cols A character vector of column names. Please ensure that you only choose column names of non-numeric columns
#' @param response The column containing the response variable.
#' @param functions A (named) list of functions to be used to generate statistics. Will take a vector and should return a scalar, e.g. mean / sd.
#' If names are provided, the name will be prepended to the generate column. If they are not provided, gen<index of function>_ will be prepended.
#' @param interaction_level Either a 1 or 2. Should we gather statistics only for one column, or also for combinations of columns?
#' @param too_few_observations_cutoff An integer denoting the minimum required observations for a combination of values in statistics_col to be used.
#' If not enough observations are present, the statistics will be generated on the entire response column. Default: 30.
#' @return A named list containing the modified train dataset and the statistics table.
#'
#' #' @details This function will also generate default values for all generated columns that use the entire response column.
#' This allows us to ensure no NA values will be present in generated columns when, for instance, a new category is detected or when values are cut-off by
#' \code{too_few_observations_cutoff}.
#'
#' @export
feature_create_all_generic_stats <- function(train, stat_cols = colnames(train)[purrr::map_lgl(train, is.character)], response, functions, interaction_level = 1, too_few_observations_cutoff = 30) {
    stopifnot(interaction_level == 1 || interaction_level == 2)

    L = length(stat_cols)
    tables <- as.list(1:(L + (interaction_level == 2) * (L * (L-1)) / 2))
    defaults <- as.list(1:(L + (interaction_level == 2) * (L * (L-1)) / 2))
    tables_index <- 1L
    for(colname in stat_cols){
        stats <- feature_create_generic_stats(
            train = train,
            statistics_col = colname,
            response = response,
            functions = functions,
            too_few_observations_cutoff = too_few_observations_cutoff)
        train <- stats$train

        tables[[tables_index]] <- stats$table
        defaults[[tables_index]] <- stats$defaults
        tables_index %<>% {.+1L}
    }
    if(interaction_level > 1){
        for(i in 1:(L-1)) for(j in (i+1):L){
            stats <- feature_create_generic_stats(
                train = train,
                statistics_col = stat_cols[c(i,j)],
                response = response,
                functions = functions,
                too_few_observations_cutoff = too_few_observations_cutoff)
            train <- stats$train

            tables[[tables_index]] <- stats$table
            tables_index %<>% {.+1L}
        }
    }

    # create default values
    target <- unlist(train[response])
    defaults <- purrr::map_dbl(.x = functions, .f = function(x, y) x(y), y = target)
    names(defaults) <- names(functions)

    predict_function <- function(data) feature_create_predict(data = data, tables = tables, stat_cols = stat_cols, interaction_level = interaction_level, defaults = defaults)
    result <- list("train" = train, tables = tables, "stat_cols" = stat_cols, "interaction_level" = interaction_level, .predict = predict_function, defaults = defaults)
    return(result)
}

#' Calculates stats based on custom functions on the response variable for each group provided in stat_cols.
#'
#' @param train The train dataset, as a data.frame.
#' @param statistics_col A character vector of column names. Please ensure that you only choose column names of non-numeric columns or numeric columns with few values.
#' Combinations that generate too few (<30)
#' @param response The column containing the response variable.
#' @param functions A (named) list of functions to be used to generate statistics. Will take a vector and should return a scalar, e.g. mean / sd.
#' If names are provided, the name will be prepended to the generate column. If they are not provided, gen<index of function>_ will be prepended.
#' @param too_few_observations_cutoff An integer denoting the minimum required observations for a combination of values in statistics_col to be used.
#' If not enough observations are present, the statistics will be generated on the entire response column. Default: 30.
#'
#' @details This function will also generate default values for all generated columns that use the entire response column.
#' This allows us to ensure no NA values will be present in generated columns
#'
#' @importFrom data.table as.data.table
#' @import dplyr
#' @return A named list containing the modified train dataset and the statistics table.
feature_create_generic_stats <- function(train, statistics_col, response, functions, too_few_observations_cutoff = 30){
    if(is.null(names(functions))) names(functions) <- paste("gen", 1:length(functions))

    var_names <- paste0(names(functions), "_", paste0(statistics_col, collapse = "_"))

    train_target <- train %>% dplyr::select_(.dots = statistics_col, response)
    train_count_table <- train_target %>% dplyr::group_by_(.dots = statistics_col) %>%
        dplyr::summarize(feature_create_generic_stats_count = n()) %>%
        dplyr::filter(feature_create_generic_stats_count >= too_few_observations_cutoff) %>%
        dplyr::select(-feature_create_generic_stats_count)

    statistics_train <- train_target %>% as.data.table %>%
        .[, c(var_names) := purrr::map(.x = functions, .f = function(x) x(get(response, pos = .SD))), by = statistics_col] %>%
        as.data.frame %>% select_(paste0("-", response)) %>% unique %>% right_join(train_count_table, by = statistics_col)
    train %<>% dplyr::left_join(statistics_train, by = statistics_col)

    target <- unlist(train[response])
    for(i in seq_along(functions)) {
        value <- functions[[i]](target)
        stat_col_name <- var_names[i]
        missing_index <- is.na(train[stat_col_name])
        if(any(missing_index)) train[missing_index, stat_col_name] <- value
    }

    defaults <- purrr::map_dbl(.x = functions, .f = function(x, y) x(y), y = target)
    names(defaults) <- names(functions)

    return(list("train" = train, table = statistics_train, defaults = defaults))
}

#' Uses previous statistics results to generate columns for a new dataset.
#'
#' @param data A new dataset to join with the tables generated by \code{\link{feature_create_all_generic_stats}}
#' @param stat_cols A character vector of column names. Please ensure that you only choose column names of non-numeric columns
#' @param tables The tables from \code{\link{feature_create_all_generic_stats}}.
#' @param interaction_level Interaction level that was used to generate the tables.
#' @param defaults Default values for each function, with names matching each function
#'
#' @return The \code{data} dataset, with the new columns.
#' @export
feature_create_predict <- function(data, stat_cols, tables, interaction_level, defaults) {
    L <- length(stat_cols)
    stopifnot(
        is.data.frame(data),
        !any(!stat_cols %in% colnames(data)),
        interaction_level == 1 || interaction_level == 2,
        length(tables) == (L + (interaction_level == 2) * (L * (L-1)) / 2)
    )

    tables_index <- 1L
    for(colname in stat_cols){
        data %<>% dplyr::left_join(y = tables[[tables_index]], by = colname)

        stat_col_names <- paste0(names(defaults), "_", colname)
        for(i in seq_along(defaults)) {
            stat_col_name <- stat_col_names[i]
            missing_index <- is.na(data[stat_col_name])
            if(any(missing_index)) data[missing_index, stat_col_name] <- defaults[i]
        }

        tables_index <- tables_index +1L
    }
    if(interaction_level > 1){
        for(i in 1:(L-1)) for(j in (i+1):L){
            data %<>% dplyr::left_join(y = tables[[tables_index]], by = stat_cols[c(i,j)])

            stat_col_names <- paste0(names(defaults), "_", colname)
            for(i in seq_along(defaults)) {
                stat_col_name <- stat_col_names[i]
                missing_index <- is.na(data[stat_col_name])
                if(any(missing_index)) data[missing_index, stat_col_name] <- defaults[i]
            }

            tables_index <- tables_index +1L
        }
    }
    return(data)
}

#' Remove all columns that have only a single value
#'
#' @param train train data frame to select columns from to remove.
#' @param na_function A function that returns true when a value is considered missing, and should be removed. Defaults to removing no values.
#'
#' @return The same data frame, without single-value columns
#' @export
remove_single_value_columns <- function(train, na_function = function(x){F}){
    keep_cols <- purrr::map_dbl(train, function(x) {
        res <- unique(x)
        res <- res[!na_function(res)]
        return(length(res))
    }) %>% magrittr::is_greater_than(1L) %>%
        .[.] %>%
        names

    train %<>% .[,keep_cols, drop = F]
    predict_function <- function(data) preserve_columns_predict(data = data, preserved_columns = keep_cols)
    return(list("train" = train, "preserved_columns" = keep_cols, .predict = predict_function))
}

#' Only keep previously selected columns
#'
#' @param data New data frame to remove same columns from.
#' @param preserved_columns A vector of columns that should be preserved
#'
#' @details To be used by \code{\link{remove_single_value_columns}} and \code{\link{cor_remove_high_correlation_features}}
#'
#' @return The same data frame, without single-value columns
#' @export
preserve_columns_predict <- function(data, preserved_columns) {
    return(data[,preserved_columns[preserved_columns %in% colnames(data)], drop = F])
}

#' Generates permutation interaction effects between sets of numeric variables
#'
#' @param train Data frame containing the train data.
#' @param response The column containing the response variable.
#' @param columns Columns to use for interaction effects. Can be a character vector referencing numeric columns, or an integer larger than 2 denoting
#' the minimum number of unique values in any column except \code{response} for that column to be considered for interaction effect. This is intended to exclude
#' ordinal columns using numeric representations.
#' @param max_interactions The maximum number of columns that will be considered for interaction effects per variable. E.g. for the value 3, this function
#' will generate ALL 2-way interactions between variables and ALL 3-way interactions between variables. Caution is advised to not set this value too high. Defaults to 2.
#'
#' @details Thanks Eduardo.
#' @return A named list containing the modified train and the used columns and column means for reproducability.
#' @export
feature_interactions <- function(train, response, columns = 10L, max_interactions = 2){
    if(is.numeric(columns) && columns >= 2) {
        columns <- purrr::map_lgl(train, function(x){
            if(!is.numeric(x)) return(FALSE)
            number_of_uniques <- length(unique(x))
            return(number_of_uniques >= columns)
        })
        columns <- colnames(train)[columns] %>%
            .[. != response]
    }
    stopifnot(
        is.data.frame(train),
        is.character(columns), !any(!columns %in% colnames(train)),
        !any(!purrr::map_lgl(train[, columns], is.numeric)),
        is.numeric(max_interactions) && max_interactions >= 2
    )

    col_means <- purrr::map_dbl(train[, columns], mean, na.rm = T)

    modified_train <- train %>% select_(.dots = columns)
    for(col in columns){
        modified_train[, col] <- modified_train[, col] - col_means[col]
    }

    for(level in seq_len(max_interactions - 1) + 1){
        combinations <- combn(x = columns, m = level)

        for(column_set in seq_len(ncol(combinations))){
            column_set <- combinations[, column_set]
            col_name <- paste0("interaction_", paste0(collapse = "_", column_set))

            train[, col_name] <- modified_train %>% select_(.dots = column_set) %>% apply(1, prod)
        }
    }

    predict_function <- function(data) feature_interactions_predict(data = data, column_means = col_means, columns = columns, max_interactions = max_interactions)
    return(list(
        "train" = train,
        "column_means" = col_means,
        "columns" = columns,
        "max_interactions" = max_interactions,
        .predict = predict_function
    ))
}

#' Computes interaction effects for a new dataset
#'
#' @param data Data frame with the new data
#' @param columns Column names from the \code{\link{feature_interactions}}'s results
#' @param column_means Column means from the \code{\link{feature_interactions}}'s results
#' @param max_interactions The maximum number of columns that will be considered for interaction effects per variable. Should be the same as the one used for
#' the results of \code{\link{feature_interactions}}.
#'
#' @return The data frame in \code{data} with the interaction effects
#' @export
feature_interactions_predict <- function(data, columns, column_means, max_interactions){
    stopifnot(
        is.data.frame(data),
        is.character(columns), !any(!columns %in% colnames(data)),
        !any(!purrr::map_lgl(data[, columns], is.numeric)),
        is.numeric(column_means), !any(!columns %in% names(column_means)),
        is.numeric(max_interactions) && max_interactions >= 2
    )

    modified_data <- data %>% dplyr::select_(.dots = columns)
    for(col in columns){
        modified_data[, col] <- modified_data[, col] - column_means[col]
    }

    for(level in seq_len(max_interactions - 1) + 1){
        combinations <- combn(x = columns, m = level)
        for(column_set in seq_len(ncol(combinations))){
            column_set <- combinations[, column_set]
            col_name <- paste0("interaction_", paste0(collapse = "_", column_set))
            data[, col_name] <- modified_data %>% select_(.dots = column_set) %>% apply(1, prod)
        }
    }
    return(data)
}
