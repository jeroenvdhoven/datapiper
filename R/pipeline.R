#' Create a train/test pipeline from individual functions
#'
#' @param ... Pipe segments. Each pipe segment is a list containing at least a \code{.segment} argument, which holds the function. Other parts of the list will be treated as additional arguments to that function.
#'
#' These arguments are evaluated at time of calling (so once you call the pipeline function), however if you wish to create arguments based on the datasets just before starting the processing,
#' remember you can always wrap a pipe with another function so you can do the calculations there.
#'
#' The function should always accept at least a \code{train} argument for the train dataset.
#' It should also return a list with (at a minimum) two named items: \code{train} and \code{.predict}, the function used to reconstruct the pipeline for new pipes.
#' @param response Since \code{response} is a parameter often used in this package, you can set it here to have it automatically set in pipeline where needed.
#'
#' @details Since this function returns a \code{.predict} function, it should be possible to use pipelines within pipelines.
#'
#' Note: when using custom pipeline functions, especially those with complex prediction functions, use \code{\link{create_predict_function}}. It will ensure
#' that the generated pipeline can find your prediction function when it is packaged and loaded in a clean environment.
#'
#' @return A function, taking as arguments \code{train}. This function will return a list of the transformed \code{train} dataset after running it through all pipeline functions,
#' as well as a function that reproduces the process for new data and a list containing the parameters of each pipeline segment.
#' @export
#'
#' @examples
#' library(dplyr)
#' set.seed(1)
#' train <- data_frame(a = 1:10, b = sample(c(1,2, NA), size = 10,
#'     replace = TRUE), c = sample(c(1,2), size = 10, replace = TRUE))
#' test <- data_frame(a = 1:10, b = sample(c(1,2, NA), size = 10,
#'     replace = TRUE), c = sample(c(1,2), size = 10, replace = TRUE))
#'
#' pipeline = pipeline(
#'     list(.segment = datapiper::feature_NA_indicators),
#'     list(.segment = datapiper::impute_all, exclude_columns = "a"),
#'     list(.segment = datapiper::cor_remove_high_correlation_features, exclude_columns = "a"),
#'     list(.segment = datapiper::feature_create_all_generic_stats, stat_cols = "b",
#'          response = "a", functions = list("mean" = mean, "sd" = sd),
#'          too_few_observations_cutoff = 0)
#' )
#' res <- pipeline(train)
#' trained_pipeline <- res$.predict
#' test <- trained_pipeline(test)
pipeline <- function(..., response){
    pipes <- list(...)
    # TODO: add prediction pipeline?
    # TODO: add CV pipeline
    # If any pipe doesn't contain a .segment variable that is a function, quit
    stopifnot(
        !any(!purrr::map_lgl(pipes, is.list)),
        missing(response) || is.character(response)
    )
    has_response <- !missing(response)

    if(
        any(!purrr::map_lgl(pipes, function(x) ".segment" %in% names(x))) |
        any(!purrr::map_lgl(pipes, function(x) is.function(x$.segment)))
    ) stop('Error: must provide .segment containing a function per pipe')

    if(any(!purrr::map_lgl(pipes, function(x) {
        argument_names <- names(formals(x$.segment))

        return("train" %in% argument_names)
    }))) stop("Error: one of your functions doesn't take a 'train' argument")

    res <- function(train) {
        pipeline <- list()
        pipeline_params <- list()
        mandatory_variables <- c("train", ".predict")

        # Construct and train the pipeline
        for (pipe_ in pipes) {
            f <- pipe_$.segment
            other_args <- pipe_[names(pipe_) != ".segment"]
            other_args$train <- train

            # Set response if needed
            if(has_response && !"response" %in% names(other_args) && "response" %in% formalArgs(def = f))
                other_args <- c(other_args, list(response = response))

            # Set the other arguments to the defaults for the function f, remove the ... argument.
            defaults <- formals(f) %>% .[!names(.) %in% names(other_args)] %>% .[names(.) != "..."]
            other_args <- c(other_args, defaults)

            pipe_res <- do.call(what = f, args = other_args)

            # Check if pipe_res contains train arguments
            if(!is.list(pipe_res) || is.data.frame(pipe_res) || any(!mandatory_variables %in% names(pipe_res))) {
                faulty_index <- which(purrr::map_lgl(pipes, function(x) identical(x$.segment, f)))
                missing_names <- mandatory_variables[!mandatory_variables %in% names(pipe_res)]
                stop(paste("Error: function", faulty_index, "did not return a list containing", paste(mandatory_variables, collapse = ", "), "\nMissing:", paste(missing_names, collapse = ", ")))
            }
            train <- pipe_res$train
            pipeline_params <- c(pipeline_params, list(pipe_res[!names(pipe_res) %in% c("train", ".predict")]))
            pipeline <- c(pipeline, pipe_res$.predict)
        }

        # Construct the trained pipeline function
        trained_pipeline <- function(data) {
            for(pipe_ in pipeline){
                data <- pipe_(data)
            }
            return(data)
        }
        return(list("train" = train, ".predict" = trained_pipeline, "pipe_segments" = pipeline))
    }
    return(res)
}

#' A simple wrapper for creating a pipe segment
#'
#' @param .segment The function to train a part of the pipeline. Will be checked for being a function and taking a \code{train} argument.
#' @param ... Other arguments to \code{.segment}.
#'
#' @return A list with .segment and the argument in ..., ready for \code{\link{pipeline}}
#' @export
#'
#' @examples
#' p <- segment(pipeline_mutate, a = "1")
#' p2 <- segment(pipeline_mutate, "-a")
segment <- function(.segment, ...) {
    res <- list(...)
    stopifnot(
        is.function(.segment),
        "train" %in% formalArgs(.segment),
        !"" %in% names(res)
    )
    return(c(res, .segment = .segment))
}

#' Wrapper function to turn a dplyr function into a pipeline element
#'
#' @param dplyr_function A function like select_ or mutate_ from dplyr
#' @param stop_on_missing_names Flag indicating if the pipeline should stop if ... arguments aren't named. Useful for mutate, not so much for select or group_by
#'
#' @return A wrapper function for the specified dplyr_function
#' @export
pipeline_dplyr <- function(dplyr_function, stop_on_missing_names = F) {
    return(function(train, ...) {
        if(stop_on_missing_names){
            arg_names <- names(list(...))
            stopifnot(!is.null(arg_names), !"" %in% arg_names)
        }

        train <- dplyr_function(train, ...)

        predict_function <- function(data) dplyr_function(data, ...)
        return(list(train = train, .predict = predict_function))
    })
}

#' Applies select in a pipeline
#'
#' @param train Data frame containing the train data.
#' @param ... Variables to be dropped / selected, as used in \code{\link[dplyr]{select_}}. This means you should provide arguments as strings: "col_1".
#'
#' @return A list of the transformed train dataset and a .predict function to be used on new data.
#' @export
pipeline_select <- pipeline_dplyr(select_, stop_on_missing_names = F)

#' Applies mutate in a pipeline
#'
#' @param train Data frame containing the train data.
#' @param ... Variables to be made, as used in \code{\link[dplyr]{mutate_}}. This means you should provide arguments as strings: var = "a + b".
#'
#' @return A list of the transformed train dataset and a .predict function to be used on new data.
#' @export
pipeline_mutate <- pipeline_dplyr(mutate_, stop_on_missing_names = T)

#' Wrapper for putting a single function into a pipeline
#'
#' @param train Data frame containing the train data.
#' @param f The function to be put into the pipeline. It is important that the function can be applied to new datasets without using any information from the train dataset, e.g. lowercasing column names.
#' @param ... Additional arguments to be provided to \code{f}
#'
#' @return A list of the transformed train dataset and a .predict function to be used on new data.
#' @export
#'
#' @examples
#' data <- dplyr::data_frame(var = 0, Var = 0, camelCase = 0, good_name = 0,
#'                           `0none.` = 0, `bad  ` = 0, `j&d` = 0, `spac ed` = 0)
#' pipeline_function(data, standard_column_names)
#'
#' # You can also use this to append a custom model to the pipeline
#' data <- dplyr::data_frame(x = 1:10, y = (1:10) + rnorm(10))
#' model <- lm(y ~ x, data)
#'
#' self_contained_function <- function(data) predict(model, data)
#' pipe <- pipeline_function(data, self_contained_function)
#'
#' predictions <- pipe$.predict(data)

pipeline_function <- function(train, f, ...) {
    train <- f(train, ...)

    predict_function <- function(data) f(data, ...)
    return(list(train = train, .predict = predict_function))
}

#' Create a pipeline step that learns what the data looks like
#'
#' @param train Data frame containing the train data.
#' @param response The response variable. Will be used as an optional column name.
#' Does not have to exist in the train dataset (useful when it is added in later in the pipeline)
#' @param on_missing_column What to do when a new dataset misses columns.
#' Either "error", which causes an error, or "add", which adds the missing columns with only NA's filled in.
#' @param on_extra_column What to do when a new dataset has extra column.s
#' Either "error", which causes an error, or "remove", which removes the extra columns.
#' @param on_type_error What to do when a new dataset causes warnings or errors on casting columns to new types.
#' Either "error", which causes an error, or "ignore", which ignores warnings but will still allow errors to propagate.
#'
#' @return A list of the train dataset and a .predict function to be used on new data.
#' @importFrom purrr map2_df
#' @export
pipeline_check <- function(train,
                           response,
                           on_missing_column = c("error", "add")[1],
                           on_extra_column = c("remove", "error")[1],
                           on_type_error = c("ignore", "error")[1]) {
    stopifnot(
        is.data.frame(train),
        !missing(response), is.character(response),
        on_missing_column %in% c("error", "add"),
        on_extra_column %in% c("remove", "error"),
        on_type_error %in% c("ignore", "error")
    )

    # Save column names, excluding the response
    cols = colnames(train)[colnames(train) != response]
    if(response %in% colnames(train)) train <- train[, c(response, cols)]
    else train <- train[, cols]

    # Save column types
    col_types = purrr::map_chr(train, class)

    .predict = function(data) pipeline_check_predict(data = data, response = response, cols = cols, col_types = col_types,
                                                     on_missing_column = on_missing_column, on_extra_column = on_extra_column,
                                                     on_type_error = on_type_error)
    return(list(train = train, .predict = .predict, response = response, cols = cols, col_types = col_types,
                on_missing_column = on_missing_column, on_extra_column = on_extra_column,
                on_type_error = on_type_error))
}

pipeline_check_predict <- function(data, response, cols, col_types, on_missing_column, on_extra_column, on_type_error) {
    stopifnot(is.data.frame(data))

    if(!response %in% colnames(data)) data[, response] <- NA
    cols <- c(response, cols)

    columns_present = cols %in% colnames(data)

    # If we don't see a column in the dataset that we expect to see, stop or add those columns
    if(any(!columns_present)) {
        if(on_missing_column == "add") data[, cols[!columns_present]] <- NA
        else stop(paste("Error: column(s)", paste0(collapse = ", ", "'", cols[!columns_present], "'"), "not present while expected to be present"))
    }

    # If we see a column in the dataset that we don't expect to see, stop or remove those columns.
    # If we miss only the response column, continue.
    is_expected_column = colnames(data) %in% cols

    if(any(!is_expected_column) && on_extra_column == "error"){
        stop(paste("Error: column(s)", paste0(collapse = ", ", "'", colnames(data)[!is_expected_column], "'"), "present while expected to not be present"))
    } else {
        data <- data[, cols]
    }

    # Stop if the ordering and presence of columns is not exactly the same
    stopifnot(length(cols) == ncol(data), !any(colnames(data) != cols))

    # Coerce columns to the correct format
    if(on_type_error == "error") tryCatch({
        data <- purrr::map2_df(.x = data, .y = col_types, as)
    }, warning = function(e){
         stop(paste("Error: some column tye conversions introduced conversion warnings:\n", e))
    }, error = function(e){
        stop(paste("Error: some column tye conversions introduced conversion warnings:\n", e))
    }) else data <- purrr::map2_df(.x = data, .y = col_types, as)

    return(data)
}

#' Ensures a predict function can be called for custom pipeline functions
#'
#' @param .predict_function The function that will be used for applying learned transformations to a new dataset. It should take at least
#' a \code{data} argument for new data to apply transformations to.
#' @param ... Arguments to \code{.predict_function}.
#'
#' @return A function that takes a \code{data} argument (for new data) and uses the provided arguments in \code{...}.
#' @export
#'
#' @details The predict function of custom pipeline segments can cause problems when being redeployed if the predict function is not properly supplied to the function.
#' In these cases, this helper function can assist. It will force evaluation of its function argument before using it in a new function, ensuring no
#' delayed evaluation occurs. This should prevent errors along the likes of "predict function not found" when deploying the pipeline in a clean environment.
create_predict_function <- function(.predict_function, ...) {
    force(x = .predict_function)

    stopifnot(
        is.function(.predict_function),
        "data" %in% formalArgs(.predict_function)
    )

    return(function(data) {
        .predict_function(data = data, ...)
    })
}
