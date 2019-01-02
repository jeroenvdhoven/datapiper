context("Range classifier")
describe("pipe_range_classifier", {
    def_values <- c(5, 10, 15)
    col <- "a"
    resp <- unlist(dataset1[col])
    n_even <- 5
    n_quantile <- 5

    r <- suppressWarnings(ctest_for_no_errors(to_eval = datapiper::pipe_range_classifier(dataset1, response = "x", values = def_values, exclude_columns = c("m2", "m", "z", "z2", "y")),
                        error_message = "Can't run pipe_range_classifier"))

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("creates interval classifiers", {
        expected_columns <- paste0("x_quantile_", def_values)
        ctest_dataset_has_columns(r$train, expected_columns)

        generated <- r$train[, expected_columns]
        expect_false(any(generated < 0))
        expect_false(any(generated > 1))
    })

    it("can use quantiles, even spreads or regular values", {
        r_quantile <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = col, quantiles = n_quantile, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(col, "_quantile_", quantile(x = resp, probs = seq(0, 1, length.out = n_quantile + 2))[2:(n_quantile + 1)]) %>% sort %>% unique
        generated_columns <- colnames(r_quantile$train)[grepl(pattern = "_quantile_", x = colnames(r_quantile$train))] %>% sort
        expect_equal(expected_columns, generated_columns)

        r_even <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = col, even_spreads = n_even, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(col, "_quantile_", seq(min(resp), max(resp), length.out = n_even + 2)[2:(n_even + 1)]) %>% sort %>% unique
        generated_columns <- colnames(r_even$train)[grepl(pattern = "_quantile_", x = colnames(r_even$train))] %>% sort
        expect_equal(expected_columns, generated_columns)

        r_values <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = col, values = def_values, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(col, "_quantile_", def_values) %>% sort
        generated_columns <- colnames(r_values$train)[grepl(pattern = "_quantile_", x = colnames(r_values$train))] %>% sort
        expect_equal(expected_columns, generated_columns)
    })

    it("can use quantiles, even spreads or regular values when the response has missing values", {
        na_col <- "m2"
        resp <- unlist(dataset1[na_col])
        r_quantile <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = na_col, quantiles = n_quantile, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(na_col, "_quantile_", quantile(x = resp, probs = seq(0, 1, length.out = n_quantile + 2), na.rm = T)[2:(n_quantile + 1)]) %>% sort %>% unique
        generated_columns <- colnames(r_quantile$train)[grepl(pattern = "_quantile_", x = colnames(r_quantile$train))] %>% sort
        expect_equal(expected_columns, generated_columns)

        r_even <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = na_col, even_spreads = n_even, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(na_col, "_quantile_", seq(min(resp, na.rm = T), max(resp, na.rm = T), length.out = n_even + 2)[2:(n_even + 1)]) %>% sort %>% unique
        generated_columns <- colnames(r_even$train)[grepl(pattern = "_quantile_", x = colnames(r_even$train))] %>% sort
        expect_equal(expected_columns, generated_columns)

        r_values <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = na_col, values = def_values, exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- paste0(na_col, "_quantile_", def_values) %>% sort
        generated_columns <- colnames(r_values$train)[grepl(pattern = "_quantile_", x = colnames(r_values$train))] %>% sort
        expect_equal(expected_columns, generated_columns)
    })

    it("can combine quantiles, even spreads and regular values", {
        r_all <- suppressWarnings(datapiper::pipe_range_classifier(dataset1, response = col, even_spreads = n_even, quantiles = n_quantile, values = def_values,
                                             exclude_columns = c("m2", "m", "z", "z2", "y")))
        expected_columns <- c(
            paste0(col, "_quantile_", quantile(x = resp, probs = seq(0, 1, length.out = n_quantile + 2))[2:(n_quantile + 1)]),
            paste0(col, "_quantile_", seq(min(resp), max(resp), length.out = n_even + 2)[2:(n_even + 1)]),
            expected_columns <- paste0(col, "_quantile_", def_values)
        )

        ctest_dataset_has_columns(r_all$train, expected_columns)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_range_classifier_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can use xgboost models as well, which handle missing values", {
        r_all_xgb <- pipe_range_classifier(dataset1, response = col, even_spreads = n_even, quantiles = n_quantile, values = def_values,
                                             exclude_columns = c("z", "z2", "y", "s"), model = "xgboost")
        ctest_pipe_has_working_predict_function(r_all_xgb, dataset1)
    })
})
