library(testthat)
library(magrittr)
library(dplyr)
library(datapiper)

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
#
#     it("handles missing values", {})
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
