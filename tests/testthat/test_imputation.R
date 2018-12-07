context("Imputation")
# Skeleton
describe("pipe_impute", {
    r <- ctest_for_no_errors(to_eval = pipe_impute(train = dataset1, exclude_columns = c("z", "z2"), type = "mean"),
                             error_message = "Can't run impute with mean")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("can impute using different techniques", {
        expect_true(anyNA(dataset1))
        expect_false(anyNA(r$train[, purrr::map_lgl(r$train, is.numeric)]))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for impute_predict_all()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("does not error when not setting exclude_columns and choosing mean imputation", {
        r <- ctest_for_no_errors(to_eval = pipe_impute(train = dataset1, type = "mean"),
                                 error_message = "Can't run impute with mean without setting exclude_columns")
    })

    it("can set columns to ignore while imputing", {
        excluded <- c("z", "y", "x", "z2")
        r_exclude <- pipe_impute(train = dataset1, exclude_columns = excluded, type = "lm")
        used_columns <- names(r_exclude$trees[[1]]$coefficients)
        expect_false(any(excluded %in% used_columns))
    })

    it("can set its own detection function for missing values", {
        excluded <- c("z", "y", "x", "z2")
        r_na <- pipe_impute(train = dataset1, exclude_columns = excluded, type = "lm", na_function = is.na)
        expect_false(anyNA(r_na$train[, purrr::map_lgl(r_na$train, is.numeric)]))

        r_zero <- pipe_impute(train = dataset1, exclude_columns = excluded, type = "lm", na_function = function(x) x == 0)
        expect_true(anyNA(r_zero$train[, purrr::map_lgl(r_zero$train, is.numeric)]))
    })

    it("ignores non-numeric input for xgboost by default", {
        ctest_for_no_errors(datapiper::pipe_impute(train = dataset1, type = "xgboost"),
                            error_message = "Error ignoring non-numeric input for xgboost by default")
    })

    it("ignores constant input for lm by default", {
        ctest_for_no_errors(pipe_impute(train = select(dataset1, -y), type = "lm"),
                            error_message = "ignores constant input for lm by default")
    })
})
