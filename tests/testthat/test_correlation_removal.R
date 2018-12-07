context("Correlation removal")
describe("pipe_remove_high_correlation_features", {

    r <- ctest_for_no_errors(
        to_eval = datapiper::pipe_remove_high_correlation_features(train = dataset1),
        error_message = "pipe_remove_high_correlation_features() does not work with defaults"
    )

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("removes highly correlated features", {
        expect_equal(sum(c("a", "b", "x") %in% colnames(r$train)), 1)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for preserve_columns_predict()", {
        ctest_pipe_has_working_predict_function(pipe_res = r, data = dataset1)
    })

    it("ignores non-numeric input", {
        ctest_dataset_has_columns(dataset = r$train, columns = c("y", "s", "z"))
    })

    it("can be set to ignore certain numeric columns", {
        r_sub <- datapiper::pipe_remove_high_correlation_features(train = dataset1, exclude_columns = "x")
        ctest_dataset_has_columns(dataset = r_sub$train, columns = c("x"))
        expect_equal(sum(c("a", "b") %in% colnames(r_sub$train)), 1)
    })

    it("handles missing values", {
        d <- dataset1
        d$a[5] <- NA

        r_sub <- datapiper::pipe_remove_high_correlation_features(train = d)
        expect_gte(sum(c("a", "b", "x") %in% colnames(r_sub$train)), 1)
        expect_lt(sum(c("a", "b", "x") %in% colnames(r_sub$train)), 3)
    })
})


