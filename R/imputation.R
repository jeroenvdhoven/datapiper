#' Generate a model to impute missing data in a column
#'
#' We don't recommend to impute categorical variables with many levels.
#'
#' @param data The dataset, as a data.frame.
#' @param column The column to be imputed. Should be a string.
#' @param NA_value A function to define what a NA is for this column which returns TRUE when a value is missing and FALSE otherwise. Will take one column vector as input.
#' @param exclude_columns Columns that shouldn't be included in the imputation.
#' @param train_fraction The fraction of training data that should be used for imputation vs verification. The fraction will be taken from the data that doesn't have missing values. The remaining data will be used as a test set
#' @param controls Either:
#' \itemize{
#' \item NA for defaults
#' \item A list for params from xgboost. Should always contain at least \code{nrounds}.
#' }
#' @param type The type of algorithm to use for imputation. Options: mean, lm, and xgboost. Mean will calculate the mean for numeric sets and the mode for non-numerics.
#'
#' @return The model built based on \code{type} If train_fraction is below 1, will also return the prediction results on the train and test set.
impute_tree <- function(data, column, NA_value = is.na, exclude_columns, train_fraction = .9, controls = NA, type = "xgboost"){
    if(is.function(NA_value)) missing <- NA_value(data[column])
    else stop('NA_value must be a function')
    if(sum(missing) / nrow(data) > .33) warning(paste("Warning:", sum(missing) / nrow(data) * 100L, "% of the data in column", column, "is missing\n"))

    data %<>% .[!missing,]

    set_aside <- sample.int(n = nrow(data), size = train_fraction * nrow(data))
    train_data <- data[set_aside,]
    test_data <- data[-set_aside,]


    included_cols <- colnames(data) %>% .[
        (!. %in% c(exclude_columns, column) ) &          #All columns that are not excluded.
            (!grepl(pattern = column, x = ., fixed = T)) #All columns that contain exactly the same column name.
        ]

    form <- column %>% paste0(., " ~ `", paste0(included_cols, collapse = '` + `'), "`") %>% as.formula

    if(is.na(controls)) {
        if(type == "xgboost") controls = list(nrounds = 50)
    }

    if(type == "mean"){
        target_vector <- unlist(train_data[column])
        if(is.numeric(target_vector)) tree <- mean(target_vector)
        else tree <- unique(target_vector) %>% .[which.max(tabulate(match(target_vector, .)))]
    }
    else if(type == "lm") tree <- lm(formula = form, data = train_data)
    else if(type == "xgboost") {
        reduced_data <- colnames(data) %in% c(exclude_columns, column)
        reduced_data <- data[,!reduced_data]
        xgbm <- xgboost::xgb.DMatrix(data = as.matrix(reduced_data), label = unlist(data[column]))
        tree <- xgboost::xgb.train(params = controls, nrounds = controls$nrounds, verbose = F, data = xgbm)
    }
    else error("Invalid type argument for imputation")

    if(train_fraction < 1){
        if(type == "xgboost") f = as.matrix
        else f = function(x){x}
        train_result <- predict(object = tree, newdata = train_data %>% f) %>% as.character()
        train_result <- list(original = train_data[column] %>% unlist, predicted <- train_result)
        test_result <- predict(object = tree, newdata = test_data %>% f) %>% as.character()
        test_result <- list(original = test_data[column] %>% unlist, predicted <- test_result)
        return(list(
            tree = tree,
            train_result = train_result,
            test_result = test_result
        ))
    }else{
        return(tree)
    }
}

#' Impute multiple missing columns using mean / mode, lm or xgboost
#'
#' @param train train dataset.
#' @param columns the columns to impute.
#' @param na_function what value represents missing values. Defaults to is.na. Either:
#' \itemize{
#' \item A function, which returns TRUE when a value is missing and FALSE otherwise. Will apply this function to each column. Must take one column vector as input.
#' \item A list of functions, all in the same format as above. Must be as long as \code{columns}.
#' }
#' @param exclude_columns what columns should not be used as predictors? Can be integers, logicals or column names.
#' @param type mean, tree, or xgboost.
#' @param controls controls for the decision tree or xgboost, if needed. Default to NA.
#' @param verbose whether xgboost should print anything
#'
#' @return A list of models to impute NA's in the columns.
impute_all_trees <- function(train, columns = colnames(train)[purrr::map_lgl(train, na_function)], na_function = is.na,
                             exclude_columns, type = "lm",
                             controls = NA, verbose = T){
    stopifnot(
        is.function(na_function) || is.list(na_function),
        !any(!exclude_columns %in% colnames(train)),
        is.character(columns),
        type %in% c("lm", "xgboost", "mean"),
        !any(!columns %in% colnames(train)),
        is.function(na_function)
    )

    models <- purrr::map(.x = columns, .f = function(x, train, columns, na_function, exclude_columns, controls, type){
        if(verbose) print(x)
        impute_tree(data = train, column = x, NA_value = na_function, train_fraction = 1, exclude_columns = exclude_columns, controls = controls, type = type)
    }, train = train, columns = columns, na_function = na_function, exclude_columns = exclude_columns, controls = controls, type = type)

    names(models) <- columns
    return(models)
}

#----------------------------------------------------------------#

impute_predict <- function(data, column, NA_value, tree, exclude_columns){
    if(is.function(NA_value)) missing_values <- NA_value(unlist(data[column]))
    else stop('NA_value must be a function')

    if(length(missing_values) == 0){
        warning(paste("Column", column, "has no missing values?\n"))
        return(data[column])
    }

    target <- data[column]
    data <- select_(data, .dots = paste0("-", exclude_columns))

    if(is.vector(tree) && length(tree) == 1){
        target[missing_values, column] <- tree
    } else {
        if(class(tree) == "xgb.Booster") data %<>% as.matrix
        target[missing_values, column] <- data[missing_values, , drop = F] %>% predict(object = tree, newdata = .)
        if(class(tree) == "xgb.Booster") data %<>% as.data.frame
    }
    data
    return(target)
}

#' Use the models from \code{impute_all_trees} to impute the selected columns in data.
#'
#'
#' @param data data dataset to impute.
#' @param columns the columns to impute.
#' @param na_function Function which returns TRUE when a value is missing and FALSE otherwise.
#' @param trees The models generated by \code{impute_all_trees}
#' @param exclude_columns what columns should not be used as predictors? Can be integers, logicals or column names.
#' @param verbose whether xgboost should print anything
#'
#' @return The same dataset as the imputed, but with NA values in the selected columns replaced by their estimated values.
#' @export
impute_predict_all <- function(data, columns, na_function, trees, exclude_columns, verbose = F){
    trees
    stopifnot(
        is.character(columns),
        !any(!columns %in% colnames(data)),
        is.function(na_function),
        length(trees) == length(columns)
    )

    temp <- data[,0]
    for(i in seq_along(columns)){
        if(verbose) print(columns[i])
        temp[columns[i]] <- impute_predict(data = data, column = columns[i], NA_value = na_function,
                                           tree = trees[[i]], exclude_columns = exclude_columns)
    }

    for(i in colnames(temp))
        data[i] <- temp[i]

    return(data)
}

#' Impute multiple missing columns using lm, decision trees or xgboost, and perform imputation
#'
#' @param train Train dataset.
#' @param columns The columns to impute, as strings.
#' @param na_function A function which returns TRUE when a value is missing and FALSE otherwise. Will apply this function to each column. Must take one column vector as input.
#' @param exclude_columns Columns that should not be used in imputation. If lm is chosen, this will always include \code{columns}. Should be strings.
#' @param type lm, mean, or xgboost.
#' @param controls Controls for xgboost, if needed. Default to NA.
#' @param verbose Whether xgboost should print anything.
#'
#' @return A list containing the train dataset, imputed, a .predict function, and all necesities to recreate the imputation process
#' @export
impute_all <- function(train, columns,
                       na_function = is.na, exclude_columns, type = "lm",
                       controls = NA, verbose = F){
    stopifnot(
        is.data.frame(train),
        type %in% c("lm", "mean", "xgboost"),
        is.logical(verbose)
    )

    if(missing(columns)) columns <- colnames(train)[purrr::map_lgl(train, function(x) any(na_function(x), na.rm = T) && is.numeric(x))]

    if(missing(exclude_columns)) {
        if(type == "mean") exclude_columns = character(0)
        else if(type == "lm") {
            # Ensure single value columns are not used
            keep_columns <- datapiper::remove_single_value_columns(train, na_function)$preserved_columns
            exclude_columns <- colnames(train)[!colnames(train) %in% keep_columns]
        } else if (type == "xgboost") {
            # Ensure no non-numeric columns are used
            exclude_columns <- colnames(train)[!purrr::map_lgl(train, is.numeric)]
        }
    }

    if(type == "lm"){
        # To ensure we don't generate NA's when calling predict.lm
        exclude_columns <- c(exclude_columns, columns[!columns %in% exclude_columns])
    }

    trees <- impute_all_trees(train, columns = columns, na_function = na_function, exclude_columns =
                                  exclude_columns, type = type, controls = controls, verbose = verbose)

    predict_function <- function(data) impute_predict_all(data = data, columns = columns, na_function = na_function, trees = trees,
                                                          exclude_columns = exclude_columns)
    train <- predict_function(train)
    return(list(train = train, columns = columns, na_function = na_function, trees = trees, .predict = predict_function))
}






