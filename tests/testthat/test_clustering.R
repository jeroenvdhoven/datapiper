context("Clustering")
describe("pipe_clustering()", {
    na_columns <- colnames(dataset1)[purrr::map_lgl(dataset1, anyNA)]

    it("runs without errors on basic settings", {
        r <- ctest_for_no_errors(error_message = "pipe_clustering() does not run on defaults",
                                 to_eval = pipe_clustering(train = dataset1))
        r <- ctest_for_no_errors(error_message = "pipe_clustering() does not run on defaults with exclusion",
                                 to_eval = pipe_clustering(train = dataset1, exclude_columns = na_columns))
    })

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        r <- pipe_clustering(train = dataset1, exclude_columns = na_columns)

        ctest_pipe_has_correct_fields(r)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for clustering_predict()", {
        r <- pipe_clustering(train = dataset1, exclude_columns = na_columns)
        ctest_pipe_has_working_predict_function(pipe_res = r, data = dataset1)
    })

    it("can run when not excluding missing values or non-numeric columns", {
        r <- pipe_clustering(train = dataset1, exclude_columns = character(0))
    })

    it("can run when not excluding missing values or non-numeric columns on new data", {
        r <- pipe_clustering(train = dataset1, exclude_columns = character(0))
        ctest_pipe_has_working_predict_function(pipe_res = r, data = dataset1)
    })

    it("adds cluster labels to a dataset as a character vector", {
        clust_column <- "cluster"
        test_df <- data_frame(
            x = c(rnorm(n = 100), rnorm(n = 100, mean = 10)),
            y = c(rnorm(n = 100, mean = -1, sd = 3), rnorm(n = 100, mean = 6, sd = 1))
        )

        r <- pipe_clustering(train = test_df, cluster_column = clust_column, k = 2)

        expect_true(clust_column %in% colnames(r$train))

        col <- unlist(r$train[clust_column])
        expect_true(is.character(col))
        expect_equivalent(col, c(rep("1", 100), rep("2", 100)))
    })

    it("allows you to set the number of clusters", {
        k <- 9
        r <- pipe_clustering(train = dataset1, k = k)

        expect_equal(max(as.numeric(r$train$cluster)), k)
    })

    it("allows you to set the distance metric", {
        r <- ctest_for_no_errors(error_message = "pipe_clustering() does not run with manhattan metric",
                            to_eval = pipe_clustering(train = dataset1, metric = "manhattan"))
    })

    it("can exclude selected columns from the clustering", {
        test_df <- data_frame(
            x = c(rnorm(n = 100), rnorm(n = 100, mean = 10)),
            y = c(rnorm(n = 50, mean = -5, sd = 1), rnorm(n = 100, mean = 6, sd = 1), rnorm(n = 50, mean = 30, sd = 1))
        )
        r <- pipe_clustering(train = test_df, exclude_columns = "x", k = 3)
        expect_equal(r$train$cluster, c(rep("1", 50), rep("2", 100), rep("3", 50)))
    })

    it("can use either a data.table or data.frame as input and use the result on either", {
        ctest_dt_df_compatibility(pipe_func = pipe_clustering, df = dataset1)
    })

    it("can check some common inputs", {
        ctest_if_pipes_check_common_inputs(pipe_func = pipe_clustering, data = dataset1)
    })

    it("can run on a single input", {
        r <- pipe_clustering(train = dataset1, exclude_columns = "x", k = 3)
        res <- invoke(r$pipe, data = dataset1[1,])

        expect_equal(object = res, expected = r$train[1,])
    })
})
