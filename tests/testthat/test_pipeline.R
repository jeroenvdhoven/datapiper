describe("segment", {
    r <- segment(.segment = pipeline_mutate, andere = "a + 1")
    it("generates a list with a .segment element", {
        expect_true(is.list(r))
        expect_true(".segment" %in% names(r))
        expect_true("andere" %in% names(r))
    })

    it("generates errors when the input is incorrect", {
        expect_error(info = "Expect crash on not giving a function", object = segment(.segment = "A"))
        expect_error(info = "Expect crash on not giving a function with a train argument", object = segment(.segment = mean))
    })
})

describe("pipeline_dplyr", {
    it("generates a pipeline function for dplyr functions when used", {
        r <- pipeline_dplyr(select_)
        expect_true(is.function(r))
        expect_true("train" %in% formalArgs(r))
    })
})

describe("pipeline_select", {
    r <- pipeline_select(dataset1, "-c")
    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("selects columns as needed", {
        ctest_dataset_does_not_have_columns(r$train, "c")

        r_keep <- pipeline_select(dataset1, "c")
        expect_equal(r_keep$train, dataset1[, 'c'])
    })

    it("can apply its results to a new dataset using .predict, a wrapper for pipeline_select_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })
})

describe("pipeline_mutate", {
    r <- pipeline_mutate(dataset1, q = "c + 1")
    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("mutates columns as needed", {
        ctest_dataset_has_columns(r$train, "q")
        expect_equal(r$train$q, dataset1$c + 1)
    })

    it("can apply its results to a new dataset using .predict, a wrapper for pipeline_mutate_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("crashses when no argument names are provided", {
        expect_error(info = "Expect crash on passing unnamed variables (singular)", object = pipeline_mutate(dataset1, 5))
        expect_error(info = "Expect crash on passing unnamed variables (multiple)", object = pipeline_mutate(dataset1, 5, 6))
        expect_error(info = "Expect crash on passing unnamed variables (combined)", object = pipeline_mutate(dataset1, 5, p = "5"))
    })
})

describe("pipeline_function", {
    applied_function <- function(d) return(d[, purrr::map_lgl(d, is.numeric)])
    r <- pipeline_function(dataset1, f = applied_function)
    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("applies a function to the entire dataset and returns the transformed dataset", {
        expect_false(any(!purrr::map_lgl(r$train, is.numeric)))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for pipeline_function_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can provide additional arguments that will be fed into the provided function", {
        filter_by <- function(d, func) return(d[, purrr::map_lgl(d, func)])
        r_with_arg <- pipeline_function(dataset1, f = filter_by, func = is.character)

        expect_false(any(!purrr::map_lgl(r_with_arg$train, is.character)))
    })
})

describe("pipeline_check", {
    r <- ctest_for_no_errors(
        pipeline_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore"),
        error_message = "pipeline_check does not work with defaults")

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("does not need to have response in its column names", {
        ctest_for_no_errors(
            pipeline_check(dataset1, response = "another column", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore"),
            error_message = "pipeline_check does not work when the response is missing")
    })

    it("saves the state of the current dataframe", {
        expect_false(any(!r$cols %in% colnames(dataset1)))
        expect_false("x" %in% r$cols)
        expect_equal(length(r$cols), ncol(dataset1) - 1) # For missing response

        expect_equal(purrr::map_chr(dataset1, class), purrr::map_chr(r$train, class))
        expect_equal(purrr::map_chr(dataset1, class), r$col_types)
    })

    it("can always handle missing responses", {
        r_missing <- pipeline_check(dataset1, response = "x", on_missing_column = "error", on_extra_column = "remove", on_type_error = "ignore")
        t_ <- ctest_for_no_errors(r_missing$.predict(select(dataset1, -x)),
                                  error_message = "Expected no crash when response is missing")
    })

    it("will error when a column is not found in a new dataset", {
        r_missing <- pipeline_check(dataset1, response = "x", on_missing_column = "error", on_extra_column = "remove", on_type_error = "ignore")
        expect_error(r_missing$.predict(select(dataset1, -a)), info = "Expected crash on missing column when configured as such")

        r_additional <- pipeline_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "error", on_type_error = "ignore")
        expect_error(r_additional$.predict(mutate(dataset1, new = "new column")), info = "Expected crash on additional column when configured as such")

        r_type <- pipeline_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "error")
        expect_error(r_type$.predict(mutate(dataset1, a = "b")), info = "Expected crash on type error when configured as such")
        t_ <- ctest_for_no_errors(r_type$.predict(mutate(dataset1, a = as.character(a))),
                            error_message = "Expected no crash when types are converted without problem")
    })

    it("generates an NA dataset when the most lenient settings are used with identical types", {
        empty_dataset <- r$.predict(data_frame(new_column = 1))
        ctest_dataset_has_columns(empty_dataset, colnames(r$train))

        expect_equal(purrr::map_chr(r$train, class), purrr::map_chr(empty_dataset, class))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for pipeline_check_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })
})

describe("pipeline()", {
    r <- ctest_for_no_errors(
        pipeline(
            segment(.segment = pipeline_mutate, a = "5"),
            segment(.segment = pipeline_select, "-b")
        ), error_message = "Can't make pipeline"
    )
    pipe_result <- r(dataset1)

    it("can take pipe segments and turn them into one function", {
        expect_true(is.function(r))
        expect_true(formalArgs(r) == "train")
    })

    it("generates a train dataset and .predict function on calling", {
        ctest_pipe_has_correct_fields(pipe_result)
    })

    it("trains a pipeline that can be applied to new data", {
        piped <- pipe_result$.predict(dataset1)
        ctest_dataset_does_not_have_columns(dataset = piped, "b")
        expect_equal(piped$a, rep(5, N))
    })

    it("can take the result of one pipeline and pipe it into another", {
        r_2 <- pipeline(
            segment(.segment = r),
            segment(.segment = pipeline_select, "c", "x")
        )

        pipe_2 <- r_2(dataset1)
        expect_equal(pipe_2$train, dataset1[, c("c", "x")])
    })

    it("checks the input for correctness", {
        # all have .segment, all are functions, all have train arguments
        expect_error(datapiper::pipeline(1, list()), info = "Will crash if any input is not a list")

        expect_error(datapiper::pipeline(list(.segment = ""), list()), info = "Will crash if any input doesn't have a .segment entry")

        expect_error(datapiper::pipeline(list(.segment = function(x) {}), list(.segment = "")),
                     info = "Will crash if any input doesn't have a .segment entry that is a function")

        expect_error(datapiper::pipeline(list(.segment = function(train) {}), list(.segment = function(x) {})),
                     info = "Will crash if any input doesn't have a .segment entry that is a function with a train argument")
    })

    it("can automatically add `response` to functions that need it but don't have it assigned yet", {
        p_missing <- ctest_for_no_errors(pipeline(
            segment(.segment = datapiper::feature_scaler),
            response = "x"
        ), error_message = "pipeline errored when adding `response` variable")
        r_missing <- ctest_for_no_errors(p_missing(dataset1), error_message = "executing pipeline with `response` variable resulted in an error")
        ctest_pipe_has_correct_fields(r_missing)

        p_no_missing <- ctest_for_no_errors(pipeline(
            segment(.segment = datapiper::feature_scaler, response = "x")
        ), error_message = "pipeline errored when not adding `response` variable")
        r_no_missing <- ctest_for_no_errors(p_no_missing(dataset1), error_message = "executing pipeline without `response` variable resulted in an error")
        ctest_pipe_has_correct_fields(r_no_missing)
    })
})

describe("create_predict_function()", {
    it("saves a function for reuse later, even when removing the original function from memory", {
        f <- function(data) data[1,]
        saved_f <- create_predict_function(.predict_function = f)

        without_removing_result <- saved_f(dataset1)
        rm(f)
        with_removing_result <- saved_f(dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })

    it("can forward parameters to .predict_function", {
        f <- function(data, rows) data[rows,]
        saved_f <- create_predict_function(.predict_function = f, rows = 1:10)

        without_removing_result <- saved_f(dataset1)
        rm(f)
        with_removing_result <- saved_f(dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })

    it("can forward calculated parameters to .predict_function", {
        f <- function(data, rows) data[rows,]
        rows <- 1:10
        saved_f <- create_predict_function(.predict_function = f, rows = rows + 1)

        without_removing_result <- saved_f(dataset1)
        rm(f, rows)
        with_removing_result <- saved_f(dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })
})

