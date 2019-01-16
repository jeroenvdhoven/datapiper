#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param train The train dataset, as a data.frame.
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

    final_columns <- character(0)
    for(col in columns){
        x = train[, col]

        y_missing <- condition(x)
        if(any(y_missing) || force_column) {
            train[paste(col, "NA_indicator", sep = "_")] <- y_missing
            final_columns <- c(final_columns, col)
        }
    }

    predict_pipe <- pipe(.function = NA_indicators_predict, columns = final_columns, condition = condition)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Indicate which fields are NA
#'
#' Takes the dataset, scans the given columns for values that have been listed as indicating NA, and adds a column indicating if this is a NA
#'
#' @param data The new dataset, as a data.frame.
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

    for(col in columns){
        x = data[, col]
        y_missing <- condition(x)
        data[paste(col, "NA_indicator", sep = "_")] <- y_missing
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
#' @return A list containing the transformed train dataset and a trained pipe.
#'
#' #' @details This function will also generate default values for all generated columns that use the entire response column.
#' This allows us to ensure no NA values will be present in generated columns when, for instance, a new category is detected or when values are cut-off by
#' \code{too_few_observations_cutoff}.
#'
#' @export
#' @importFrom data.table as.data.table
pipe_create_stats <- function(train, stat_cols = colnames(train)[purrr::map_lgl(train, is.character)], response, functions, interaction_level = 1, too_few_observations_cutoff = 30) {
    stopifnot(interaction_level == 1 || interaction_level == 2)

    L = length(stat_cols)
    tables <- as.list(1:(L + (interaction_level == 2) * (L * (L-1)) / 2))
    defaults <- as.list(1:(L + (interaction_level == 2) * (L * (L-1)) / 2))
    tables_index <- 1L
    if(!is.data.table(train)) train_dt <- as.data.table(train)
    else train_dt <- train

    for(colname in stat_cols){
        stats <- create_stats(
            train = train_dt,
            statistics_col = colname,
            response = response,
            functions = functions,
            too_few_observations_cutoff = too_few_observations_cutoff)

        tables[[tables_index]] <- stats$table
        defaults[[tables_index]] <- stats$defaults
        tables_index <- tables_index + 1L
    }
    if(interaction_level > 1){
        for(i in 1:(L-1)) for(j in (i+1):L){
            stats <- create_stats(
                train = train_dt,
                statistics_col = stat_cols[c(i,j)],
                response = response,
                functions = functions,
                too_few_observations_cutoff = too_few_observations_cutoff)

            tables[[tables_index]] <- stats$table
            tables_index <- tables_index + 1L
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
#' @importFrom data.table is.data.table
#' @import dplyr
#' @return A list containing the transformed train dataset, a .predict function to repeat the process on new data and all parameters needed to replicate the process.
create_stats <- function(train, statistics_col, response, functions, too_few_observations_cutoff = 30){
    if(is.null(names(functions))) names(functions) <- paste("gen", 1:length(functions))
    stopifnot(data.table::is.data.table(train))

    var_names <- paste0(names(functions), "_", paste0(statistics_col, collapse = "_"))

    train_target <- train[, .SD, .SDcols = c(statistics_col, response)]
    train_count_table <- train[, .N, by = statistics_col][N >= too_few_observations_cutoff][
        , .SD, .SDcols = statistics_col]

    statistics_train <- train_target[, c(var_names) := purrr::map(.x = functions, .f = function(x) x(get(response, pos = .SD))), by = statistics_col][
        , c(response) := NULL] %>% unique %>%
        merge(y = train_count_table, by = statistics_col)

    train <- merge(train, statistics_train, by = statistics_col)

    target <- unlist(train[, response, with = F])
    for(i in seq_along(functions)) {
        value <- functions[[i]](target)
        stat_col_name <- var_names[i]
        missing_index <- train[, is.na(get(stat_col_name))]
        if(any(missing_index)) train[missing_index, c(stat_col_name) := value]
    }

    defaults <- purrr::map_dbl(.x = functions, .f = function(x, y) x(y), y = target)
    names(defaults) <- names(functions)

    return(list("train" = train, table = statistics_train, defaults = defaults))
}

#' Uses previous statistics results to generate columns for a new dataset.
#'
#' @param data A new dataset to join with the tables generated by \code{\link{pipe_create_stats}}
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
        interaction_level == 1 || interaction_level == 2,
        # Ensure the number of tables is what we'd expect
        length(tables) == (L + (interaction_level == 2) * (L * (L-1)) / 2)
    )

    is_dt <- data.table::is.data.table(data)

    tables_index <- 1L
    for(colname in stat_cols){
        if(is_dt) data %<>% merge(y = tables[[tables_index]], by = colname, all.x = T)
        else data %<>% dplyr::left_join(y = tables[[tables_index]], by = colname, all.x = T)

        stat_col_names <- paste0(names(defaults), "_", colname)
        for(i in seq_along(defaults)) {
            stat_col_name <- stat_col_names[i]
            if(is_dt) missing_index <- as.vector(is.na(data[, stat_col_name, with = F]))
            else missing_index <- is.na(data[stat_col_name])
            if(any(missing_index)) {
                if(is_dt) data[missing_index, c(stat_col_name) := defaults[i]]
                else data[missing_index, stat_col_name] <- defaults[i]
            }
        }

        tables_index <- tables_index +1L
    }
    if(interaction_level > 1){
        for(i in 1:(L-1)) for(j in (i+1):L){
            next_table <- tables[[tables_index]]
            cols <- colnames(next_table)[1:2]
            if(is_dt) data %<>% merge(y = next_table, by = cols, all.x = T)
            else data %<>% dplyr::left_join(y = tables[[tables_index]], by = cols, all.x = T)

            stat_col_names <- paste0(names(defaults), "_", colname)
            for(i in seq_along(defaults)) {
                stat_col_name <- stat_col_names[i]

                if(is_dt) missing_index <- as.vector(is.na(data[, stat_col_name, with = F]))
                else missing_index <- is.na(data[stat_col_name])
                if(any(missing_index)) {
                    if(is_dt) data[missing_index, c(stat_col_name) := defaults[i]]
                    else data[missing_index, stat_col_name] <- defaults[i]
                }
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
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
pipe_remove_single_value_columns <- function(train, na_function = function(x){F}){
    more_than_one_unique_value <- purrr::map_lgl(train, function(x) {
        res <- unique(x)
        res <- res[!na_function(res)]
        return(length(res) > 1L)
    })
    keep_cols <- names(more_than_one_unique_value[more_than_one_unique_value])

    train %<>% select_(.dots = keep_cols)

    predict_pipe <- pipe(.function = preserve_columns_predict, preserved_columns = keep_cols)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Only keep previously selected columns
#'
#' @param data New data frame to remove same columns from.
#' @param preserved_columns A vector of columns that should be preserved
#'
#' @details To be used by \code{\link{pipe_remove_single_value_columns}} and \code{\link{pipe_remove_high_correlation_features}}
#'
#' @return The same data frame, without single-value columns
preserve_columns_predict <- function(data, preserved_columns) {
    return(data[,preserved_columns[preserved_columns %in% colnames(data)], drop = F])
}

#' Generates permutation interaction effects between sets of numeric variables
#'
#' @param train Data frame containing the train data.
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
        !any(!purrr::map_lgl(train[, columns], is.numeric)),
        is.numeric(max_interactions) && max_interactions >= 2
    )

    col_means <- purrr::map_dbl(train[, columns], mean, na.rm = T)

    modified_train <- train[, columns]
    for(col in columns){
        modified_train[, col] <- modified_train[, col] - col_means[col]
    }

    for(level in seq_len(max_interactions - 1) + 1){
        combinations <- combn(x = columns, m = level)

        for(column_set in seq_len(ncol(combinations))){
            column_set <- combinations[, column_set]
            col_name <- paste0("interaction_", paste0(collapse = "_", column_set))

            train[, col_name] <- apply(modified_train[, column_set], 1, prod)
        }
    }

    predict_pipe <- pipe(.function = feature_interactions_predict, column_means = col_means, columns = columns, max_interactions = max_interactions)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Computes interaction effects for a new dataset
#'
#' @param data Data frame with the new data
#' @param columns Column names from the \code{\link{pipe_feature_interactions}}'s results
#' @param column_means Column means from the \code{\link{pipe_feature_interactions}}'s results
#' @param max_interactions The maximum number of columns that will be considered for interaction effects per variable. Should be the same as the one used for
#' the results of \code{\link{pipe_feature_interactions}}.
#'
#' @return The data frame in \code{data} with the interaction effects
feature_interactions_predict <- function(data, columns, column_means, max_interactions){
    stopifnot(
        is.data.frame(data),
        is.character(columns), !any(!columns %in% colnames(data)),
        !any(!purrr::map_lgl(data[, columns], is.numeric)),
        is.numeric(column_means), !any(!columns %in% names(column_means)),
        is.numeric(max_interactions) && max_interactions >= 2
    )

    modified_data <- data[, columns]
    for(col in columns){
        modified_data[, col] <- modified_data[, col] - column_means[col]
    }

    for(level in seq_len(max_interactions - 1) + 1){
        combinations <- combn(x = columns, m = level)
        for(column_set in seq_len(ncol(combinations))){
            column_set <- combinations[, column_set]
            col_name <- paste0("interaction_", paste0(collapse = "_", column_set))
            data[, col_name] <- apply(modified_data[, column_set], 1, prod)
        }
    }
    return(data)
}
