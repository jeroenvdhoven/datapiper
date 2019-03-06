library(testthat)
library(magrittr)
library(dplyr)
library(datapiper)
library(data.table)

N <- 30
set.seed(2)
x = seq_len(N)
# dataset1 <- data.table::data.table(
dataset1 <- dplyr::data_frame(
    x = x,
    # "1stweirdcolumn" = (N - x) ^ 1.2,
    a = x^2,
    b = log(x),
    c = sample.int(n = 10, size = N, replace = T),
    y = letters[(x %% 26) + 1],
    s = sample(x = c("A", "B"), size = N, replace = T),
    m = sample(x = c(1:5, NA), size = N, replace = T),
    m2 = sample(x = c(1:5, NA), size = N, replace = T),
    z = "A",
    boolean = sample(c(T,F), size = N, replace = T),
    z2 = sample(c("A", NA), size = N, replace = T)
)

# Skeleton
# describe("<function_name>", {
#     it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {})
#     it("does the thing it is supposed to do", {})
#     it("can apply its results to a new dataset using pipe, a wrapper for <function_name>_predict()", {})
#     it("ignores non-numeric input", {})
#     it("runs without errors on basic settings", {})
#     it("check some common inputs", {
#       ctest_if_pipes_check_common_inputs(pipe_func = <pipe_function>, data = dataset1)
#     })
#
#     it("handles missing values", {})
#     it("can take data.table and data.frame as input and for predictions", {}) # Make sure this test covers all flows in your function. ctest_dt_df can help for pipes
# })

ctest_pipe_has_correct_fields <- function(pipe_res) {
    expect_false(is.null(pipe_res$train))
    expect_false(is.null(pipe_res$pipe))
}

ctest_dataset_has_columns <- function(dataset, columns) {
    expect_false(any(!columns %in% colnames(dataset)))
}

ctest_dataset_does_not_have_columns <- function(dataset, columns) {
    expect_false(any(columns %in% colnames(dataset)))
}

ctest_pipe_has_working_predict_function <- function(pipe_res, data) {
    piped <- invoke(pipe_res$pipe, data)
    expect_equal(piped, pipe_res$train)
}

ctest_for_no_errors <- function(to_eval, error_message ){
    stopifnot(!missing(error_message))
    tryCatch({
        return(to_eval)
    }, error = function(e) {
        expect_true(F, info = error_message)
    })
    return(NULL)
}

util_RMSE <- function(labels, predicted) {
    sqrt(mean((labels - predicted)^2, na.rm = T))
}

util_RMSLE <- function(labels, predicted, base = 2) {
    sqrt(mean((log(labels, base = base) - log(predicted, base = base)) ^ 2, na.rm = T))
}

util_MAE <- function(labels, predicted) {
    mean(abs(labels - predicted))
}


ctest_dt_df <- function(pipe_func, dt, df, train_by_dt = T, .check_post_pipe = F, ...) {
    dt_copy <- data.table::copy(dt)

    if(train_by_dt) train_df <- dt_copy else train_df <- df
    r <- pipe_func(train = train_df, ...)

    invoked_dt <- invoke(x = r$pipe, dt)
    invoked_df <- invoke(x = r$pipe, df)

    if(train_by_dt) {
        expect_true(is.data.table(r$train))
    } else {
        expect_false(is.data.table(r$train))
        expect_true(is.data.frame(r$train))
    }

    expect_true(is.data.table(invoked_dt))
    expect_false(is.data.table(invoked_df))
    expect_true(is.data.frame(invoked_df))

    expect_equal(invoked_df, as_data_frame(invoked_dt))

    if(.check_post_pipe) {
        expect_true("post_pipe" %in% names(r), label = "Post-pipe not computed when requested")
        retransformed_dt <- invoke(x = r$post_pipe, invoked_dt)
        retransformed_df <- invoke(x = r$post_pipe, invoked_df)

        expect_true(is.data.table(retransformed_dt))
        expect_false(is.data.table(retransformed_df))
        expect_true(is.data.frame(retransformed_df))

        expect_equal(retransformed_df, as_data_frame(retransformed_dt))
    }
}

ctest_if_pipes_check_common_inputs <- function(pipe_func, data, ...) {
    pipe_name <- deparse(match.call()$pipe_func)
    arguments <- formalArgs(pipe_func)
    default_label <- paste("Pipe", pipe_name, "did not check some default inputs:")

    # train: present and dataframe
    expect_true("train" %in% arguments)
    expect_error(pipe_func(...), regexp = 'train', fixed = T, label = default_label)
    expect_error(pipe_func(train = "", ...), regexp = 'is.data.frame(train)', fixed = T, label = default_label)

    # response: is.character, %in% colnames(data)
    if("response" %in% arguments) {
        expect_error(pipe_func(train = data, response = 1, ...), regexp = 'is.character(response)', fixed = T, label = default_label)

        cols <- colnames(data)
        definitely_not_present_column_names <- paste0(cols[nchar(cols) == max(nchar(cols))], "_someotherstuff")
        expect_error(pipe_func(train = data, response = definitely_not_present_column_names, ...), regexp = 'response %in% colnames(train)',
                     fixed = T, label = default_label)
    }

    # exclude_columns: is.character, %in% colnames(data)
    if("exclude_columns" %in% arguments) {
        if("response" %in% arguments) expect_error(pipe_func(train = data, response = colnames(data)[1], exclude_columns = 1, ...),
                                                   regexp = 'exclude_columns.*is not TRUE', fixed = F, label = default_label)
        else expect_error(pipe_func(train = data, exclude_columns = 1, ...), regexp = 'exclude_columns.*is not TRUE', fixed = F, label = default_label)

        cols <- colnames(data)
        definitely_not_present_column_names <- paste0(cols[nchar(cols) == max(nchar(cols))], "_someotherstuff")

        if("response" %in% arguments) expect_error(pipe_func(train = data, response = colnames(data)[1], exclude_columns = definitely_not_present_column_names, ...),
                                                   regexp = 'exclude_columns.*is not TRUE', fixed = F, label = default_label)
        else expect_error(pipe_func(train = data, exclude_columns = definitely_not_present_column_names, ...),
                     regexp = 'exclude_columns.*is not TRUE', fixed = F, label = default_label)
    }
}
