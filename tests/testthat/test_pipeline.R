context("Pipelines")
describe("segment", {
    r <- segment(.segment = pipe_mutate, andere = "a + 1")
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

### Pipe

describe("pipe", {
    additional_argument <- "a + 1"
    r <- pipe(.function = range_predict, models = additional_argument)
    it("generates a pipe object with predict_function and args elements, of which andere will be part", {
        expect_false(any(class(r) != c("pipe", "list")))
        expect_true("predict_function" %in% names(r))
        expect_true("args" %in% names(r))
        expect_true("models" %in% names(r$args))
        expect_equal(additional_argument, r$args$models)
    })

    it("generates errors when the input is incorrect", {
        expect_error(info = "Expect crash on not giving a function", object = pipe(.function = "A"))
        expect_error(info = "Expect crash on not giving a function with a data argument", object = pipe(.function = mean))
        expect_error(info = "Expect crash on giving a wrong argument to the function", object = pipe(.function = range_predict, a = 1))
        expect_error(info = "Expect crash on giving a non-named argument when the function does not allow it", object = pipe(.function = range_predict, 1))
        expect_error(info = "Expect crash on giving a data argument", object = pipe(.function = range_predict, data = 1))
    })

    it("saves a function for reuse later, even when removing the original function from memory", {
        f <- function(data) data[1,]
        saved_f <- pipe(.function = f)

        without_removing_result <- invoke(saved_f, dataset1)
        rm(f)
        with_removing_result <- invoke(saved_f, dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })

    it("saves a function's arguments for reuse later, even when removing the original arguments from memory", {
        f <- function(data, i) data[i,]
        saved_f <- pipe(.function = f, i = nrow(dataset1))

        without_removing_result <- invoke(saved_f, dataset1)
        rm(f)
        with_removing_result <- invoke(saved_f, dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })

    it("can forward parameters to .predict_function", {
        f <- function(data, rows) data[rows,]
        saved_f <- pipe(.function = f, rows = 1:10)

        without_removing_result <- invoke(saved_f, dataset1)
        rm(f)
        with_removing_result <- invoke(saved_f, dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })

    it("can forward calculated parameters to .predict_function", {
        f <- function(data, rows) data[rows,]
        rows <- 1:10
        saved_f <- pipe(.function = f, rows = rows + 1)

        without_removing_result <- invoke(saved_f, dataset1)
        rm(f, rows)
        with_removing_result <- invoke(saved_f, dataset1)

        expect_equal(without_removing_result, with_removing_result)
    })
})

describe("is.pipe", {
    it("return true when an object is a pipe and false otherwise", {
        r <- pipe(.function = range_predict, models = "1")
        expect_true(is.pipe(r))
        expect_false(is.pipe(1))
        expect_false(is.pipe(list()))
    })
})

describe("invoke.pipe", {
    it("applies new data to a trained pipe", {
        add_one <- function(data, value) data + value
        r <- pipe(.function = add_one, value = 10)

        result <- invoke(r, 1:10)
        expect_equal((1:10) + 10, result)
    })
})

### Pipeline

describe("pipeline", {
    additional_argument <- "a + 1"
    p1 <- pipe(.function = range_predict, models = additional_argument)
    p2 <- pipe(.function = range_predict, models = additional_argument)

    it("runs with basic settings", {
        ctest_for_no_errors(pipeline(p1, p2), error_message = "Simple pipeline doesn't work")
        ctest_for_no_errors(pipeline(p1, p2, pipeline(p1, p2)), error_message = "Simple pipeline doesn't work")
    })

    r <- pipeline(p1, p2)
    r_nested <- pipeline(p1, p2, r)

    it("generates a pipeline from pipes", {
        expect_false(any(class(r) != c("pipeline", "list")))
        expect_false(any(!purrr::map_lgl(.x = r, .f = ~ is.pipe(.) || is.pipeline(.))))

        expect_false(any(class(r_nested) != c("pipeline", "list")))
        expect_false(any(!purrr::map_lgl(.x = r_nested, .f = ~ is.pipe(.) || is.pipeline(.))))
    })

    it("generates errors when the input is incorrect", {
        expect_error(info = "Expect crash on not giving a function", object = pipeline("A"))
    })

    it("sets default names for pipes", {
        r <- pipeline(p1, p2)
        expect_named(r, expected = paste0("pipe_", seq_len(2)))
    })

    it("allows you to set custom names for pipeline segments", {
        r <- pipeline("my_pipe" = p1, "second_pipe" = p2)
        expect_named(r, expected = c("my_pipe", "second_pipe"))
    })

    it("fills in blanks when you dont name all pipe segments", {
        r <- pipeline("my_pipe" = p1, p2)
        expect_named(r, expected = c("my_pipe", "pipe_1"))
    })
})

describe("is.pipeline", {
    it("return true when an object is a pipe and false otherwise", {
        p1 <- pipe(.function = range_predict, models = "1")
        p2 <- pipe(.function = range_predict, models = "2")
        r <- pipeline(p1, p2)
        r_nested <- pipeline(p1, p2, r)

        expect_true(is.pipeline(r))
        expect_true(is.pipeline(r_nested))
        expect_false(is.pipeline(1))
        expect_false(is.pipeline(p1))
        expect_false(is.pipeline(list()))
    })
})

describe("invoke.pipeline", {
    it("applies new data to a trained pipe", {
        add_one <- function(data, value) data + value
        p1 <- pipe(.function = add_one, value = 10)
        p2 <- pipe(.function = add_one, value = -20)

        r <- pipeline(p1, p2)
        r_nested <- pipeline(p1, p2, r)

        result <- invoke(r, 1:10)
        expect_equal((1:10) + 10 - 20, result)

        result <- invoke(r_nested, 1:10)
        expect_equal((1:10) + (10 - 20) * 2, result)
    })

    it("can print updates if requested", {
        add_one <- function(data, value) data + value
        p1 <- pipe(.function = add_one, value = 10)
        p2 <- pipe(.function = add_one, value = -20)

        r <- pipeline(p1, p2)
        r_nested <- pipeline(p1, p2, a_custom_pipe_name = r)

        expect_output(invoke(r, 1:10, verbose = T))
        expect_output(invoke(r_nested, 1:10, verbose = T), regexp = "a_custom_pipe_name")
    })
})


# Pipeline pieces
describe("pipe_dplyr", {
    it("generates a pipeline function for dplyr functions when used", {
        r <- pipe_dplyr(select_)
        expect_true(is.function(r))
        expect_true("train" %in% formalArgs(r))
    })
})

describe("pipe_select", {
    r <- pipe_select(dataset1, "-c")
    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("selects columns as needed", {
        ctest_dataset_does_not_have_columns(r$train, "c")

        r_keep <- pipe_select(dataset1, "c")
        expect_equal(r_keep$train, dataset1[, 'c'])
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_select_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })
})

describe("pipe_mutate", {
    r <- pipe_mutate(dataset1, q = "c + 1")
    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("mutates columns as needed", {
        ctest_dataset_has_columns(r$train, "q")
        expect_equal(r$train$q, dataset1$c + 1)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_mutate_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("crashses when no argument names are provided", {
        expect_error(info = "Expect crash on passing unnamed variables (singular)", object = pipe_mutate(dataset1, 5))
        expect_error(info = "Expect crash on passing unnamed variables (multiple)", object = pipe_mutate(dataset1, 5, 6))
        expect_error(info = "Expect crash on passing unnamed variables (combined)", object = pipe_mutate(dataset1, 5, p = "5"))
    })
})

describe("pipe_function", {
    applied_function <- function(data) return(data[, purrr::map_lgl(data, is.numeric)])
    r <- pipe_function(dataset1, f = applied_function)
    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("applies a function to the entire dataset and returns the transformed dataset", {
        expect_false(any(!purrr::map_lgl(r$train, is.numeric)))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_function_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can provide additional arguments that will be fed into the provided function", {
        filter_by <- function(data, func) return(data[, purrr::map_lgl(data, func)])
        r_with_arg <- pipe_function(dataset1, f = filter_by, func = is.character)

        expect_false(any(!purrr::map_lgl(r_with_arg$train, is.character)))
    })
})

describe("pipe_check", {
    r <- ctest_for_no_errors(
        pipe_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore"),
        error_message = "pipe_check does not work with defaults")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_check_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("does not need to have response in its column names", {
        ctest_for_no_errors(
            pipe_check(dataset1, response = "another column", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore"),
            error_message = "pipe_check does not work when the response is missing")
    })

    it("saves the state of the current dataframe", {
        params <- r$pipe$args
        expect_false(any(!params$cols %in% colnames(dataset1)))
        expect_false("x" %in% params$cols)
        expect_equal(length(params$cols), ncol(dataset1) - 1) # For missing response

        expect_equal(purrr::map_chr(dataset1, class), purrr::map_chr(r$train, class))
        expect_equal(purrr::map_chr(select(dataset1, -x), class), params$col_types)
    })

    it("can always handle missing responses", {
        r_missing <- pipe_check(dataset1, response = "x", on_missing_column = "error", on_extra_column = "remove", on_type_error = "ignore")
        t_ <- ctest_for_no_errors(invoke(r_missing$pipe, select(dataset1, -x)),
                                  error_message = "Expected no crash when response is missing")
    })

    it("will error when a column is not found in a new dataset", {
        r_missing <- pipe_check(dataset1, response = "x", on_missing_column = "error", on_extra_column = "remove", on_type_error = "ignore")
        expect_error(invoke(r_missing$pipe, select(dataset1, -a)), info = "Expected crash on missing column when configured as such")

        r_additional <- pipe_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "error", on_type_error = "ignore")
        expect_error(invoke(r_additional$pipe, mutate(dataset1, new = "new column")), info = "Expected crash on additional column when configured as such")

        r_type <- pipe_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "error")
        expect_error(invoke(r_type$pipe, mutate(dataset1, a = "b")), info = "Expected crash on type error when configured as such")

        t_ <- ctest_for_no_errors(invoke(r_type$pipe, mutate(dataset1, a = as.character(a))),
                            error_message = "Expected no crash when types are converted without problem")
    })

    it("generates an NA dataset when the most lenient settings are used with identical types", {
        empty_dataset <- invoke(r$pipe, data_frame(new_column = 1))
        ctest_dataset_has_columns(empty_dataset, colnames(r$train)[colnames(r$train) != "x"])

        expect_equal(purrr::map_chr(select(r$train, -x), class), purrr::map_chr(empty_dataset, class))
    })

    it("can handle missing response values in new datasets", {
        r <- pipe_check(dataset1, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore")
        invoked_result <- invoke(r$pipe, select(dataset1, -x))

        expect_named(object = invoked_result, expected = colnames(dataset1)[colnames(dataset1) != "x"])
        expect_false("x" %in% colnames(invoked_result))
    })

    it("can handle missing response values in both datasets", {
        no_x <- select(dataset1, -x)
        r <- pipe_check(no_x, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore")
        invoked_result <- invoke(r$pipe, no_x)

        expect_named(object = invoked_result, expected = colnames(no_x))
        expect_false("x" %in% colnames(invoked_result))
    })

    it("can handle missing response values in the train dataset", {
        no_x <- select(dataset1, -x)
        r <- pipe_check(no_x, response = "x", on_missing_column = "add", on_extra_column = "remove", on_type_error = "ignore")
        invoked_result <- invoke(r$pipe, dataset1)

        expect_named(object = invoked_result, expected = colnames(dataset1))
        expect_true("x" %in% colnames(invoked_result))
        expect_equal(invoked_result$x, dataset1$x)
    })
})

describe("train_pipeline()", {
    r <- ctest_for_no_errors(
        train_pipeline(
            segment(.segment = pipe_mutate, a = "5"),
            segment(.segment = pipe_select, "-b")
        ), error_message = "Can't make pipeline"
    )
    pipe_result <- r(dataset1)

    it("can take pipe segments and turn them into one function", {
        expect_true(is.function(r))
        expect_true(formalArgs(r) == "train")
    })

    it("generates a train dataset and pipe function on calling", {
        ctest_pipe_has_correct_fields(pipe_result)
    })

    it("trains a pipeline that can be applied to new data", {
        piped <- invoke(pipe_result$pipe, dataset1)
        ctest_dataset_does_not_have_columns(dataset = piped, "b")
        expect_equal(piped$a, rep(5, N))
    })

    it("can take the result of one pipeline and pipe it into another", {
        r_2 <- train_pipeline(
            segment(.segment = r),
            segment(.segment = pipe_select, "c", "x")
        )

        pipe_2 <- r_2(dataset1)
        expect_equal(pipe_2$train, dataset1[, c("c", "x")])
    })

    it("checks the input for correctness", {
        # all have .segment, all are functions, all have train arguments
        expect_error(datapiper::train_pipeline(1, list()), info = "Will crash if any input is not a list")

        expect_error(datapiper::train_pipeline(list(.segment = ""), list()), info = "Will crash if any input doesn't have a .segment entry")

        expect_error(datapiper::train_pipeline(list(.segment = function(x) {}), list(.segment = "")),
                     info = "Will crash if any input doesn't have a .segment entry that is a function")

        expect_error(datapiper::train_pipeline(list(.segment = function(train) {}), list(.segment = function(x) {})),
                     info = "Will crash if any input doesn't have a .segment entry that is a function with a train argument")
    })

    it("can automatically add `response` to functions that need it but don't have it assigned yet", {
        p_missing <- ctest_for_no_errors(train_pipeline(
            segment(.segment = datapiper::pipe_one_hot_encode),
            response = "x"
        ), error_message = "pipeline errored when adding `response` variable")
        r_missing <- ctest_for_no_errors(p_missing(dataset1), error_message = "executing pipeline with `response` variable resulted in an error")
        ctest_pipe_has_correct_fields(r_missing)

        p_no_missing <- ctest_for_no_errors(train_pipeline(
            segment(.segment = datapiper::pipe_one_hot_encode, response = "x")
        ), error_message = "pipeline errored when not adding `response` variable")
        r_no_missing <- ctest_for_no_errors(p_no_missing(dataset1), error_message = "executing pipeline without `response` variable resulted in an error")
        ctest_pipe_has_correct_fields(r_no_missing)
    })

    it("can set names for pipeline segments", {
        r <- train_pipeline(
            "mutate_a" = segment(.segment = pipe_mutate, a = "5"),
            "unselect_b" = segment(.segment = pipe_select, "-b")
        )

        pipe_result <- r(dataset1)
        expect_named(object = pipe_result$pipe, c("mutate_a", "unselect_b"))
    })

    it("will set pipe names to pipe_<i> if no names are provided", {
        r <- train_pipeline(
            segment(.segment = pipe_mutate, a = "5"),
            segment(.segment = pipe_select, "-b")
        )

        pipe_result <- r(dataset1)
        expect_named(object = pipe_result$pipe, c("pipe_1", "pipe_2"))
    })

    it("will set pipe names to pipe_<i> if there are missing names", {
        r <- train_pipeline(
            "mutate_a" = segment(.segment = pipe_mutate, a = "5"),
            segment(.segment = pipe_mutate, x = "x + 1"),
            "unselect_b" = segment(.segment = pipe_select, "-b"),
            segment(.segment = pipe_mutate, x = "x - 1")
        )

        pipe_result <- r(dataset1)
        expect_named(object = pipe_result$pipe, c("mutate_a", "pipe_1", "unselect_b", "pipe_2"))
    })

    it("can train the post-pipeline when needed", {
        retransform_columns <- c("a", "b")
        r <- train_pipeline(
            segment(.segment = pipe_scaler, exclude_columns = "x", retransform_columns = retransform_columns),
            segment(.segment = pipe_feature_transformer, response = "x", retransform_columns = retransform_columns,
                                                        transform_functions = list("sqrt" = sqrt, "log" = log))
        )

        pipe_result <- r(dataset1)
        post_transformed <- invoke(pipe_result$post_pipe, pipe_result$train)

        for(col in retransform_columns) expect_equal(unlist(post_transformed[col]), unlist(dataset1[col]))
    })

    it("can nest post-pipelines when needed", {
        retransform_columns <- c("a", "b")
        r <- train_pipeline(
            segment(.segment = pipe_scaler, exclude_columns = "x", retransform_columns = retransform_columns),
            segment(.segment = pipe_feature_transformer, response = "x", retransform_columns = retransform_columns,
                    transform_functions = list("sqrt" = sqrt, "log" = log))
        )

        r_nested <- train_pipeline(
            segment(.segment = r),
            segment(.segment = pipe_scaler, exclude_columns = "x", retransform_columns = retransform_columns, type = "N(0,1)")
        )

        pipe_result <- r(dataset1)
        post_transformed <- invoke(pipe_result$post_pipe, pipe_result$train)

        for(col in retransform_columns) expect_equal(unlist(post_transformed[col]), unlist(dataset1[col]))
    })

    it("can train the post-pipeline when needed with appropriate names", {
        retransform_columns <- c("a", "b")
        r <- train_pipeline(
            "scaler" = segment(.segment = pipe_scaler, exclude_columns = "x", retransform_columns = retransform_columns),
            "x+1" = segment(.segment = pipe_mutate, x = "x + 1"),
            "transformer" = segment(.segment = pipe_feature_transformer, response = "x", retransform_columns = retransform_columns,
                    transform_functions = list("sqrt" = sqrt, "log" = log))
        )

        pipe_result <- r(dataset1)
        post_transformed <- invoke(pipe_result$post_pipe, pipe_result$train)

        expect_equal(names(pipe_result$post_pipe), c("post_transformer", "post_scaler"))
    })
})

describe("flatten_pipeline()", {
    p_1 <- datapiper::train_pipeline(
        segment(.segment = datapiper::pipe_categorical_filter, threshold_function = function(x) 2, response = "x"),
        segment(.segment = datapiper::pipe_remove_single_value_columns, na_function = is.na),
        segment(.segment = datapiper::pipe_one_hot_encode),
        segment(.segment = datapiper::pipe_impute, exclude_columns = "x", type = "mean")
    )
    r_1 <- p_1(dataset1)$pipe

    it("takes only a pipeline as argument", {
        expect_error(object = flatten_pipeline(1), regexp = "is.pipeline(p) is not TRUE", fixed = T)
        dummy_ <- ctest_for_no_errors(to_eval = flatten_pipeline(r_1), error_message = "Error: flatten_pipeline did not run on a pipeline")
    })

    it("can take a flat pipeline as return the same pipeline", {
        flat_1 <- flatten_pipeline(r_1)
        expect_equal(flat_1, r_1)
    })

    it("will not change the transformations done by the pipeline", {
        flat_1 <- flatten_pipeline(r_1)

        piped_original <- invoke(r_1, dataset1)
        piped_flatten <- invoke(flat_1, dataset1)

        expect_equal(piped_original, piped_flatten)
    })

    # Nested in this context means there's a pipeline within the pipeline
    it("can take a nested pipeline and return a flattened pipeline", {
        p_2 <- datapiper::train_pipeline(
            segment(.segment = p_1),
            last_segment = segment(.segment = datapiper::pipe_scaler, exclude_columns = "x")
        )
        r_2 <- p_2(dataset1)$pipe
        flat_2 <- flatten_pipeline(r_2)

        expect_true(length(flat_2) == length(r_1) + 1)
        piped_original <- invoke(r_2, dataset1)
        piped_flatten <- invoke(flat_2, dataset1)

        expect_equal(piped_original, piped_flatten)
    })

    it("will warn the user when duplicate names are used", {
        p_3 <- datapiper::train_pipeline(
            segment(.segment = p_1),
            segment(.segment = datapiper::pipe_scaler, exclude_columns = "x")
        )
        r_3 <- p_3(dataset1)$pipe
        flat_3 <- expect_warning(flatten_pipeline(r_3), "Result has duplicate names")
        flat_3 <- suppressWarnings(flatten_pipeline(r_3))

        expect_true(length(flat_3) == length(r_1) + 1)
        piped_original <- invoke(r_3, dataset1)
        piped_flatten <- invoke(flat_3, dataset1)

        expect_equal(piped_original, piped_flatten)
    })

    it("can print updates if requested", {
        expect_output(p_1(dataset1, verbose = T), regexp = "(Training pipe_\\d+ \\.\\.\\.\\n)+")
    })
})
