#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param train The train dataset, as a data.frame or data.table.
#' @param condition Function to test if a value is missing. Should return true when a value is missing and false otherwise.
#' @param columns Names of the columns to check for missing values.
#' @param force_column If true, always add new columns, even if no missing values were found.
#'
#' @details Generated columns will be of type logical.
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
#'
#' @import magrittr
#' @importFrom purrr map
pipe_NA_indicators <- function(train, condition = is.na, columns = colnames(train), force_column = F){
    stopifnot(
        is.data.frame(train),
        is.function(condition),
        is.character(columns),
        !any(!columns %in% colnames(train)),
        is.logical(force_column)
    )

    if(force_column) final_columns <- columns[]
    else {
        if(is.data.table(train)) any_missing <- unlist(train[, lapply(.SD, function(x) any(condition(x))), .SDcols = columns])
        else any_missing <- purrr::map_lgl(columns, .f = ~ any(condition(train[, .])))
        final_columns <- columns[any_missing]
    }

    predict_pipe <- pipe(.function = NA_indicators_predict, columns = final_columns, condition = condition)
    train <- invoke(predict_pipe, train)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param data The new dataset, as a data.frame or data.table.
#' @param condition Function to test if a value is missing. Should return true when a value is missing and false otherwise.
#' @param columns The columns to check for missing values. Can be provided as logicals, integers, or characters
#'
#' @return The dataset with NA's properly processed.
#'
#' @import magrittr
#' @importFrom purrr map
NA_indicators_predict <- function(data, condition, columns){
    stopifnot(
        is.data.frame(data),
        is.function(condition),
        is.character(columns),
        !any(!columns %in% colnames(data))
    )

    if(is.data.table(data)) data[, paste(columns, "NA_indicator", sep = "_") := lapply(.SD, function(x) condition(x)), .SDcols = columns]
    else for(col in columns){
        x = unlist(data[, col])
        y_missing <- condition(x)
        data[paste(col, "NA_indicator", sep = "_")] <- y_missing
    }
    return(data)
}

#' Generic function for creating statistics on the response column, based on custom columns.
#'
#' @param train The train dataset, as a data.frame or data.table.
#' @param stat_cols A character vector of column names. Please ensure that you only choose column names of non-numeric columns
#' @param response The column containing the response variable.
#' @param functions A (named) list of functions to be used to generate statistics. Will take a vector and should return a scalar, e.g. mean / sd.
#' If names are provided, the name will be prepended to the generate column. If they are not provided, gen<index of function>_ will be prepended.
#' @param interaction_level An integer of 1 or higher. Should we gather statistics only for one column, or also for combinations of columns?
#' @param too_few_observations_cutoff An integer denoting the minimum required observations for a combination of values in statistics_col to be used.
#' If not enough observations are present, the statistics will be generated on the entire response column. Default: 30.
#' @return A list containing the transformed train dataset and a trained pipe.
#'
#' #' @details This function will also generate default values for all generated columns that use the entire response column.
#' This allows us to ensure no NA values will be present in generated columns when, for instance, a new category is detected or when values are cut-off by
#' \code{too_few_observations_cutoff}.
#'
#' @export
#' @importFrom data.table as.data.table
pipe_create_stats <- function(train, stat_cols = colnames(train)[purrr::map_lgl(train, is.character)], response,
                              functions = list("mean" = mean), interaction_level = 1, too_few_observations_cutoff = 30) {
    stopifnot(is.numeric(interaction_level), interaction_level >= 1)

    tables <- list()
    defaults <- list()
    if(!is.data.table(train)) train_dt <- as.data.table(train)
    else train_dt <- train

    for(il in seq_len(interaction_level)) {
        combinations <- combn(x = stat_cols, m = il)

        for(index in seq_len(ncol(combinations))) {
            cols <- combinations[, index]
            stats <- create_stats(
                train = train_dt,
                statistics_col = cols,
                response = response,
                functions = functions,
                too_few_observations_cutoff = too_few_observations_cutoff)

            tables %<>% c(list(stats$table))
            defaults %<>% c(list(stats$defaults))
        }
    }

    # create default values
    if(is.data.table(train)) target <- unlist(train[, response, with = F])
    else target <- unlist(train[response])
    defaults <- purrr::map_dbl(.x = functions, .f = function(x, y) x(y), y = target)
    names(defaults) <- names(functions)

    predict_pipe <- pipe(.function = create_stats_predict, tables = tables, stat_cols = stat_cols,
                         interaction_level = interaction_level, defaults = defaults)

    train <- invoke(predict_pipe, train)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Calculates stats based on custom functions on the response variable for each group provided in stat_cols.
#'
#' @param train The train dataset, as a data.table
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
#' @importFrom data.table is.data.table setnames
#' @import dplyr
#' @return A list containing the generated statistics tables and defaults per columns
create_stats <- function(train, statistics_col, response, functions, too_few_observations_cutoff = 30){
    if(is.null(names(functions))) names(functions) <- paste("gen", 1:length(functions))
    stopifnot(data.table::is.data.table(train))

    var_names <- paste0(names(functions), "_", paste0(statistics_col, collapse = "_"))

    train_target <- train[, .SD, .SDcols = c(statistics_col, response)]
    train_count_table <- train[, .N, by = statistics_col][N >= too_few_observations_cutoff, .SD, .SDcols = statistics_col]

    statistics_train <- train_target[, purrr::map(.x = functions, .f = function(x, col) x(col), col = get(response)), by = statistics_col]
    generated_cols <- colnames(statistics_train)[!colnames(statistics_train) %in% statistics_col]
    setnames(x = statistics_train, old = generated_cols, new = var_names)

    statistics_train %<>% merge(y = train_count_table, by = statistics_col)
    do.call(setkey, args = c(list(x = statistics_train), statistics_col))

    defaults <- train[, lapply(functions, function(x, y) x(y), y = get(response))]
    names(defaults) <- names(functions)

    return(list(table = statistics_train, defaults = defaults))
}

#' Uses previous statistics results to generate columns for a new dataset.
#'
#' @param data The new dataset, as a data.frame or data.table.
#' @param stat_cols A character vector of column names. Please ensure that you only choose column names of non-numeric columns
#' @param tables The tables from \code{\link{pipe_create_stats}}.
#' @param interaction_level Interaction level that was used to generate the tables.
#' @param defaults Default values for each function, with names matching each function
#'
#' @return The \code{data} dataset, with the new columns.
create_stats_predict <- function(data, stat_cols, tables, interaction_level, defaults) {
    L <- length(stat_cols)
    stopifnot(
        is.data.frame(data),
        !any(!stat_cols %in% colnames(data)),
        interaction_level >= 1,
        # Ensure the number of tables is what we'd expect
        length(tables) == sum(purrr::map_dbl(.x = seq_len(interaction_level), .f = choose, n = L))
    )

    is_dt <- data.table::is.data.table(data)
    tables_index <- 1
    for(il in seq_len(interaction_level)) {
        combinations <- combn(x = stat_cols, m = il)

        for(index in seq_len(ncol(combinations))) {
            columns <- combinations[, index]

            stats_table <- tables[[tables_index]]
            if(is_dt) {
                do.call(setkey, args = c(list(x = data), columns))
                var_names <- colnames(stats_table)[!colnames(stats_table) %in% columns]
                target_cols <-  paste0(collapse = ", ", "`i.", var_names, "`")
                command <- paste0('data[stats_table, c(var_names) := .(', target_cols, ')]')
                eval(parse(text = command))
            } else data %<>% dplyr::left_join(y = stats_table, by = columns, all.x = T)

            stat_col_names <- paste0(names(defaults), "_", paste0(columns, collapse = "_"))
            for(i in seq_along(defaults)) {
                stat_col_name <- stat_col_names[i]

                if(is_dt) missing_index <- as.vector(is.na(data[, stat_col_name, with = F]))
                else missing_index <- is.na(data[stat_col_name])
                if(any(missing_index)) {
                    if(is_dt) data[missing_index, c(stat_col_name) := defaults[i]]
                    else data[missing_index, stat_col_name] <- defaults[i]
                }
            }
            tables_index <- tables_index + 1
        }
    }
    return(data)
}

#' Remove all columns that have only a single value
#'
#' @param train The train dataset, as a data.frame or data.table.
#' @param na_function A function that returns true when a value is considered missing, and should be removed. Defaults to removing no values.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
pipe_remove_single_value_columns <- function(train, na_function = function(x){F}){
    more_than_one_unique_value <- purrr::map_lgl(train, function(x) {
        res <- unique(x)
        res <- res[!na_function(res)]
        return(length(res) > 1L)
    })
    keep_cols <- names(more_than_one_unique_value[more_than_one_unique_value])

    train %<>% select_cols(cols = keep_cols)

    predict_pipe <- pipe(.function = preserve_columns_predict, preserved_columns = keep_cols)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Only keep previously selected columns
#'
#' @param data The new dataset, as a data.frame or data.table.
#' @param preserved_columns A vector of columns that should be preserved
#'
#' @details To be used by \code{\link{pipe_remove_single_value_columns}} and \code{\link{pipe_remove_high_correlation_features}}
#'
#' @return The same data.frame or data.table, without single-value columns
preserve_columns_predict <- function(data, preserved_columns) {
    if(is.data.table(data)) {
        return(data[, .SD, .SDcols = preserved_columns[preserved_columns %in% colnames(data)]])
    } else {
        return(data[,preserved_columns[preserved_columns %in% colnames(data)], drop = F])
    }
}

#' Generates permutation interaction effects between sets of numeric variables
#'
#' @param train The train dataset, as a data.frame or data.table.
#' @param response The column containing the response variable.
#' @param columns Columns to use for interaction effects. Can be a character vector referencing numeric columns, or an integer larger than 2 denoting
#' the minimum number of unique values in any column except \code{response} for that column to be considered for interaction effects. This is intended to exclude
#' ordinal columns using numeric representations.
#' @param max_interactions The maximum number of columns that will be considered for interaction effects per variable. E.g. for the value 3, this function
#' will generate ALL 2-way interactions between variables and ALL 3-way interactions between variables. Caution is advised to not set this value too high. Defaults to 2.
#'
#' @details Thanks Eduardo.
#' @return A list containing the transformed train dataset and a trained pipe.
#'
#' @export
#' @importFrom utils combn
pipe_feature_interactions <- function(train, response, columns = 10L, max_interactions = 2){
    if(is.numeric(columns) && columns >= 2) {
        columns <- purrr::map_lgl(train, function(x){
            if(!is.numeric(x)) return(FALSE)
            number_of_uniques <- length(unique(x))
            return(number_of_uniques >= columns)
        })
        columns <- colnames(train)[columns]
        columns <- columns[columns != response]
    }
    stopifnot(
        is.data.frame(train),
        is.character(columns), !any(!columns %in% colnames(train)), length(columns) > 0,
        is.numeric(max_interactions) && max_interactions >= 2
    )

    if(is.data.table(train)) columns_are_numeric <- train[, lapply(.SD, is.numeric), .SDcols = columns]
    else {
        modified_train <- train[, columns]
        columns_are_numeric <- purrr::map_lgl(modified_train, is.numeric)
    }

    stopifnot(
        !any(!columns_are_numeric)
    )

    if(is.data.table(train)) col_means <- unlist(train[, lapply(.SD, mean, na.rm = T), .SDcols = columns])
    else col_means <- purrr::map_dbl(modified_train, mean, na.rm = T)

    predict_pipe <- pipe(.function = feature_interactions_predict, column_means = col_means, columns = columns, max_interactions = max_interactions)
    train <- invoke(predict_pipe, train)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Computes interaction effects for a new dataset
#'
#' @param data The new dataset, as a data.frame or data.table.
#' @param columns Column names from the \code{\link{pipe_feature_interactions}}'s results
#' @param column_means Column means from the \code{\link{pipe_feature_interactions}}'s results
#' @param max_interactions The maximum number of columns that will be considered for interaction effects per variable. Should be the same as the one used for
#' the results of \code{\link{pipe_feature_interactions}}.
#'
#' @return The data frame in \code{data} with the interaction effects
feature_interactions_predict <- function(data, columns, column_means, max_interactions){
    stopifnot(
        is.data.frame(data),
        is.character(columns), !any(!columns %in% colnames(data))
    )
    modified_data <- select(data, columns)
    stopifnot(
        !any(!purrr::map_lgl(modified_data, is.numeric)),
        is.numeric(column_means), !any(!columns %in% names(column_means)),
        is.numeric(max_interactions) && max_interactions >= 2
    )

    if(is.data.table(data)) {
        modified_data[, c(columns) := purrr::map2(.x = .SD, .y = column_means[columns], .f = function(x, y) x - y)]
    } else {
        for(col in columns){
            modified_data[, col] <- modified_data[, col] - column_means[col]
        }
    }

    for(level in seq_len(max_interactions - 1) + 1){
        combinations <- combn(x = columns, m = level)
        for(column_set in seq_len(ncol(combinations))){
            column_set <- combinations[, column_set]
            col_name <- paste0("interaction_", paste0(collapse = "_", column_set))
            command <- paste0(collapse = " * ", column_set)

            if(is.data.table(data)) data[, c(col_name) := eval(parse(text = command), envir = modified_data)]
            else data[, col_name] <- eval(parse(text = command), envir = modified_data)
        }
    }
    return(data)
}

colapply <- function(f, ..., .args) {
    stopifnot(is.function(f))
    inputs <- list(...)
    stopifnot(length(inputs) > 0 | !missing(.args))

    if(!missing(.args)) {
        stopifnot(is.list(.args))
        inputs <- c(inputs, .args)
    }
    lengths <- purrr::map_dbl(.x = inputs, .f = length)
    stopifnot(!any(lengths != lengths[1])) # Ensure all vectors are of the same length

    res <- inputs[[1]]
    inputs <- inputs[-1]
    for(vec in inputs) res <- f(res, vec)
    return(res)
}
