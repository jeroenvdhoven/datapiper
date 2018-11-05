library(testthat)
testthat::describe("feature_transformer()", {
    r <- feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("transforms features to those most linearly predictive of the response", {
        expect_equal(r$train$a, dataset1$x)
        expect_equal(r$train$b, dataset1$x)
        expect_equal(r$train$x, dataset1$x, info = "The response remains untouched")
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_transformer_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("ignores non-numeric input", {
        expect_equal(r$train$y, dataset1$y)
        expect_equal(r$train$z, dataset1$z)
    })

    it("should crash when the response is not in the dataset or non-numeric", {
        d <- select(dataset1, -x)
        expect_error(feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp)))

        d <- mutate(dataset1, x = as.character(x))
        expect_error(feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp)))
    })

    it("transforms numeric input where there is a better correlation with the response", {
        r <- feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
    })

    it("does not transform input when it creates invalid values", {
        d <- mutate(dataset1, c = c(exp(seq_len(N-1)), -1))
        r <- feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
        expect_equal(r$train$c, d$c)
    })

    it("can transform features that had NA's before", {
        d <- mutate(dataset1, c = c(exp(seq_len(N-1)), NA))
        r <- feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
        expect_equal(r$train$c, c(seq_len(N-1), NA))
    })
})

ctest_01_range <- function(col) {
    expect_equal(min(col), 0)
    expect_equal(max(col), 1)
}

ctest_normal_range <- function(col) {
    expect_equal(mean(col), 0)
    expect_equal(sd(col), 1)
}

testthat::describe("feature_scaler()", {
    r_01 <- feature_scaler(train = dataset1, exclude_columns = "x", type = "[0-1]")
    r_normal <- feature_scaler(train = dataset1, exclude_columns = "x", type = "N(0,1)")
    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r_01)
        ctest_pipe_has_correct_fields(r_normal)
    })

    it("scales numeric features to the predefined range", {
        ctest_01_range(r_01$train$a)
        ctest_01_range(r_01$train$b)

        ctest_normal_range(r_normal$train$a)
        ctest_normal_range(r_normal$train$b)
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_scaler_predict()", {
        ctest_pipe_has_working_predict_function(r_01, dataset1)
        ctest_pipe_has_working_predict_function(r_normal, dataset1)
    })

    it("ignores non-numeric input", {
        expect_equal(r_01$train$y, dataset1$y)
        expect_equal(r_01$train$z, dataset1$z)
        expect_equal(r_normal$train$y, dataset1$y)
        expect_equal(r_normal$train$y, dataset1$y)
    })

    it("errors when a dataset is passed with 0 rows", {
        expect_error(feature_scaler(train = dataset1[0, ], exclude_columns = "x", type = "N(0,1)"),
                     regexp = "nrow(train) > 0 is not TRUE", fixed = T)
    })
})

# Skeleton
testthat::describe("feature_one_hot_encode()", {
    r_pca <- feature_one_hot_encode(train = dataset1, use_pca = T, columns = c("y", "s"))
    r_non <- feature_one_hot_encode(train = dataset1, use_pca = F)

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r_non)
        ctest_pipe_has_correct_fields(r_pca)
    })

    it("can encode character columns in a one-hot way", {
        ctest_dataset_has_columns(r_non$train, columns = c(paste0("y_", dataset1$y), paste0("s_", dataset1$s), paste0("z_", dataset1$z)))

        for(col in c("s", "y", "z")) {
            values <- unique(unlist(dataset1[col]))
            for(v in values) expect_equal(as.integer(unlist(dataset1[col]) == v), unlist(r_non$train[paste0(col, "_", v)], use.names = F))
        }
    })

    it("can encode character columns in a one-hot way using PCA", {
        ctest_dataset_does_not_have_columns(r_pca$train, c("s", "y"))
        expect_true(any(grepl(pattern = "^PC[0-9]+$", x = colnames(r_pca$train))),
                    info = "one-hot encoding with PCA generated incorrect column names")
    })

    it("can use mean-encoding", {
        stats_functions <- list("mean" = mean, "quantile" = function(x) quantile(x = x, .25))
        cols <- c("y", "s")
        r_mean <- feature_one_hot_encode(train = dataset1, use_pca = F, columns = cols,
                                         stat_functions = stats_functions, response = "x")

        generated_cols <- expand.grid(cols, names(stats_functions))
        generated_cols <- paste0(generated_cols[,2], "_", generated_cols[,1])
        ctest_dataset_does_not_have_columns(dataset = r_mean$train, columns = cols)
        ctest_dataset_has_columns(dataset = r_mean$train, columns = generated_cols)

        for(col in cols){
            for(i in seq_along(stats_functions)){
                f <- stats_functions[[i]]
                stats <- group_by_(.data = dataset1, col) %>%
                    transmute(value = f(x))

                intented_col <- stats$value
                stat_col <- paste0(names(stats_functions)[i], "_", col)
                target_col <- unlist(r_mean$train[stat_col])

                expect_equivalent(expected = intented_col, object = target_col,
                                  info = paste("Is column", col, "generated correctly using function", names(stats_functions)[i]))
            }
        }

    })

    it("can use mean-encoding on new datasets", {
        stats_functions <- list("mean" = mean, "quantile" = function(x) quantile(x = x, .25))
        cols <- c("y", "s")
        r_mean <- feature_one_hot_encode(train = dataset1, use_pca = F, columns = cols,
                                         stat_functions = stats_functions, response = "x")
        ctest_pipe_has_working_predict_function(r_mean, dataset1)
    })

    it("can use mean-encoding with PCA", {
        stats_functions <- list("mean" = mean, "quantile" = function(x) quantile(x = x, .25))
        cols <- c("y", "s")
        r_mean_pca <- feature_one_hot_encode(train = dataset1, use_pca = T, columns = cols,
                                         stat_functions = stats_functions, response = "x")

        generated_cols <- expand.grid(cols, names(stats_functions))
        generated_cols <- paste0(generated_cols[,2], "_", generated_cols[,1])
        ctest_dataset_does_not_have_columns(dataset = r_mean_pca$train, columns = cols)
        ctest_dataset_does_not_have_columns(dataset = r_mean_pca$train, columns = generated_cols)

        expect_true(any(grepl(pattern = "^PC[0-9]+$", x = colnames(r_mean_pca$train))),
                    info = "mean encoding with PCA generated incorrect column names")
    })

    it("can use mean-encoding on new datasets", {
        stats_functions <- list("mean" = mean, "quantile" = function(x) quantile(x = x, .25))
        cols <- c("y", "s")
        r_mean_pca <- feature_one_hot_encode(train = dataset1, use_pca = T, columns = cols,
                                             stat_functions = stats_functions, response = "x")
        ctest_pipe_has_working_predict_function(r_mean_pca, dataset1)
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_one_hot_encode_predict()", {
        ctest_pipe_has_working_predict_function(r_non, dataset1)
        ctest_pipe_has_working_predict_function(r_pca, dataset1)
    })

    it("ignores numeric input", {
        ctest_dataset_has_columns(r_pca$train, c("a", "b", "x"))
        ctest_dataset_has_columns(r_non$train, c("a", "b", "x"))
    })
})

# Skeleton
testthat::describe("feature_categorical_filter()", {
    r <- feature_categorical_filter(train = dataset1, response = "x",
                                    insufficient_occurance_marker = "marker", threshold_function = function(data) 5)

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("filters out categorical values that occur not often enough and leaves those that do", {
        expect_equal(dataset1$s, r$train$s)
        expect_equal(dataset1$z, r$train$z)
        expect_equal(r$train$y, rep("marker", N))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_categorical_filter_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can create a pipeline that handles new values well", {
        d <- mutate(dataset1, y = "henk")
        expect_equal(invoke(r$pipe, d)$y, rep("marker", N))
    })

    it("can create a pipeline that handles missing values well", {
        d <- mutate(dataset1, y = NA_character_)
        expect_equal(invoke(r$pipe, d)$y, rep("marker", N))
    })

    it("ignores non-numeric input", {
        expect_equal(dataset1$x, r$train$x)
        expect_equal(dataset1$a, r$train$a)
        expect_equal(dataset1$b, r$train$b)
    })
})

