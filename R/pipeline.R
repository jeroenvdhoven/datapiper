#' Create a train/test pipeline from individual functions
#'
#' @param ... Pipe segments. Each pipe segment is a list containing at least a \code{.segment} argument, which holds the function.
#' Other parts of the list will be treated as additional arguments to that function.
#' \code{\link{segment}} provides a simple wrapper for these pipe segments.
#'
#' These arguments are evaluated at time of calling (so once you call the pipeline function), however if you wish to create arguments based
#' on the datasets just before starting the processing, remember you can always wrap a pipe with another function so you can do the calculations there.
#'
#' The function should always accept at least a \code{train} argument for the train dataset.
#' Each function should also return a list with (at a minimum) two named items: \code{train} and \code{pipe}, a trained pipe segment.
#' You can create these using \code{\link{pipe}}.
#'
#' @param response Since \code{response} is a parameter often used in this package, you can set it here to have it automatically set in pipeline where needed.
#'
#' @details Since this function returns a \code{pipe} entry in its list, it should be possible to use the result of this function in a new pipeline.
#'
#' @return A function, taking as arguments \code{train}. This function will return a list of the transformed \code{train} dataset after running it through all pipeline functions,
#' as well as a \code{\link{pipeline}} that reproduces the process for new data.
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
#' P <- train_pipeline(
#'     segment(.segment = datapiper::feature_NA_indicators),
#'     segment(.segment = datapiper::impute_all, exclude_columns = "a"),
#'     segment(.segment = datapiper::cor_remove_high_correlation_features, exclude_columns = "a"),
#'     segment(.segment = datapiper::feature_create_all_generic_stats, stat_cols = "b",
#'          response = "a", functions = list("mean" = mean, "sd" = sd),
#'          too_few_observations_cutoff = 0)
#' )
#' trained_pipeline <- P(train = train)$pipe
#'
#' train <- invoke(trained_pipeline, train)
#' test <- invoke(trained_pipeline, test)
#' @importFrom methods formalArgs as
train_pipeline <- function(..., response){
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
        trained_pipelines <- as.list(seq_along(pipes))
        mandatory_variables <- c("train", "pipe")

        # Construct and train the pipeline
        for (i in seq_along(pipes)) {
            pipe_ <- pipes[[i]]
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
            trained_pipelines[[i]] <- pipe_res$pipe
        }

        trained_pipeline <- do.call(what = pipeline, args = trained_pipelines)

        return(list("train" = train, "pipe" = trained_pipeline))
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
        dplyr_wrapper <- function(data, ...) dplyr_function(data, ...)

        predict_pipe <- pipe(.function = dplyr_wrapper, ...)
        return(list(train = train, pipe = predict_pipe))
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
#' It should take a \code{data} argumet
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
#' model_pipe <- pipeline_function(data, self_contained_function)
#'
#' predictions <- invoke(model_pipe$pipe, data)
pipeline_function <- function(train, f, ...) {
    stopifnot(
        is.function(f),
        "data" %in% formalArgs(f)
    )
    train <- f(data = train, ...)

    predict_pipe <- pipe(.function = f, ...)
    return(list(train = train, pipe = predict_pipe))
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
#' @return A list of the train dataset and a pipe to be used on new data.
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

    predict_pipe <- pipe(.function = pipeline_check_predict, response = response, cols = cols, col_types = col_types,
                         on_missing_column = on_missing_column, on_extra_column = on_extra_column,
                         on_type_error = on_type_error)

    return(list(train = train, pipe = predict_pipe))
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

#' Creates a pipe object out of a function and a list of arguments
#'
#' @param .function A function to repeat transformations from a trained pipeline on new data. Will be checked for being a function and taking a \code{data} argument.
#' @param ... Other arguments to \code{.function}. Non-named arguments are only accepted if \code{.function} takes a \code{...} argument. Named arguments will be checked to
#' be in the argument list of \code{.function}.
#'
#' @return A pipe object with two entries:
#' \itemize{
#' \item \code{predict_function}: A function to repeat transformations from a trained pipeline on new data.
#' \item \code{args}: Arguments for \code{predict_function}
#' }
#'
#' @export
pipe <- function(.function, ...) {
    args <- list(...)
    stopifnot(
        is.function(.function),
        "data" %in% formalArgs(.function)
    )

    if(length(args) > 0) {
        stopifnot(
            !"data" %in% names(args),
            !("" %in% names(args) || is.null(names(args))) || "..." %in% formalArgs(.function),
            names(args)[names(args) != ""] %in% formalArgs(.function) || "..." %in% formalArgs(.function)
        )
    }

    result <- list(predict_function = .function, args = args)
    class(result) <- c("pipe", "list")

    return(result)
}

#' Creates a pipeline out of a set of pipes
#'
#' @param ... Pipes create with \code{\link{pipe}} or \code{\link{pipeline}}
#'
#' @return A pipeline object, which can be applied to new data using \code{\link{invoke}}
#' @export
pipeline <- function(...) {
    pipes <- list(...)
    stopifnot(
        !any(!purrr::map_lgl(.x = pipes, ~ is.pipe(.) || is.pipeline(.)))
    )

    class(pipes) <- c("pipeline", "list")
    return(pipes)
}

#' Generic function to apply either a pipe or pipeline to new data
#'
#' @param x The pipe / pipeline to be applied
#' @param data A new dataframe
#'
#' @return The transformed dataset
#' @export
invoke <- function(x, data) UseMethod("invoke", x)


#' Applies a pipe to new data
#'
#' @param x The pipe to be applied
#' @param data A new dataframe
#'
#' @return The transformed dataset
#' @export
invoke.pipe <- function(x, data) {
    arg_list <- x$args
    arg_list$data <- data
    return(do.call(what = x$predict_function, args = arg_list))
}

#' Applies a pipeline to new data
#'
#' @param x The pipeline to be applied
#' @param data A new dataframe
#'
#' @return The transformed dataset
#' @export
invoke.pipeline <- function(x, data) {
    for(pipe_ in x){
        data <- invoke(pipe_, data)
    }

    return(data)
}

#' Tests if an object inherits from pipe
#'
#' @param x object to be tested
#'
#' @return A boolean indicating if \code{x} inherits from \code{\link{pipe}}
#' @export
is.pipe <- function(x) {
    inherits(x, "pipe")
}

#' Tests if an object inherits from pipeline
#'
#' @param x object to be tested
#'
#' @return A boolean indicating if \code{x} inherits from \code{\link{pipeline}}
#' @export
is.pipeline <- function(x) {
    inherits(x, "pipeline")
}
