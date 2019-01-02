context("Feature transformer")
library(testthat)
testthat::describe("pipe_feature_transformer()", {
    r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("transforms features to those most linearly predictive of the response", {
        expect_equal(r$train$a, dataset1$x)
        expect_equal(r$train$b, dataset1$x)
        expect_equal(r$train$x, dataset1$x, info = "The response remains untouched")
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_feature_transformer_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("ignores non-numeric input", {
        expect_equal(r$train$y, dataset1$y)
        expect_equal(r$train$z, dataset1$z)
    })

    it("should crash when the response is not in the dataset or non-numeric", {
        d <- select(dataset1, -x)
        expect_error(pipe_feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp)))

        d <- mutate(dataset1, x = as.character(x))
        expect_error(pipe_feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp)))
    })

    it("transforms numeric input where there is a better correlation with the response", {
        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
    })

    it("does not transform input when it creates invalid values", {
        d <- mutate(dataset1, c = c(exp(seq_len(N-1)), -1))
        r <- pipe_feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
        expect_equal(r$train$c, d$c)
    })

    it("can transform features that had NA's before", {
        d <- mutate(dataset1, c = c(exp(seq_len(N-1)), NA))
        r <- pipe_feature_transformer(train = d, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp))
        expect_equal(r$train$c, c(seq_len(N-1), NA))
    })

    it("can generate retransformations if requested", {
        retransform_columns <- c("a", "b", "c")

        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp),
                                      retransform_columns = retransform_columns)
        expect_true("post_pipe" %in% names(r))
        retransformed <- invoke(r$post_pipe, r$train)

        for(col in retransform_columns) expect_equal(unlist(retransformed[col]), unlist(dataset1[col]))
    })

    it("can generate retransformations for values outside of the original range, within reason", {
        new_dataset <- mutate(dataset1, a = a * 1.1, b = b * .9)
        retransform_columns <- c("a", "b")

        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp),
                                      retransform_columns = retransform_columns)
        new_transformed <- invoke(r$pipe, new_dataset)
        retransformed <- invoke(r$post_pipe, new_transformed)

        for(col in retransform_columns) expect_equal(unlist(retransformed[col]), unlist(new_dataset[col]))
    })

    it("will be able to handle impossible values gracefully", {
        new_dataset <- mutate(dataset1, a = a - 10)
        retransform_columns <- c("a")

        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp),
                                      retransform_columns = retransform_columns)
        new_transformed <- suppressWarnings(invoke(r$pipe, new_dataset))

        retransformed_with_error <- (invoke(r$post_pipe, new_transformed))
        a_where_NaN_expected <- retransformed_with_error$a[new_dataset$a < 0]
        expect_equal(a_where_NaN_expected, rep(NaN, length(a_where_NaN_expected)))

        a_where_no_NaN_expected <- retransformed_with_error$a[new_dataset$a >= 0]
        expect_equal(a_where_no_NaN_expected, new_dataset$a[new_dataset$a >= 0])
    })

    it("will return an NA when a value is too far outside of the preset range.", {
        new_dataset <- mutate(dataset1, b = b * - 100000)
        retransform_columns <- c("b")

        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp),
                                      retransform_columns = retransform_columns)
        new_transformed <- (invoke(r$pipe, new_dataset))

        expect_warning(invoke(r$post_pipe, new_transformed), regexp = "f() values at end points not of opposite sign", fixed = T)
        retransformed_with_error <- suppressWarnings(invoke(r$post_pipe, new_transformed))

        b_where_outside_range <- retransformed_with_error$b[new_dataset$b < r$post_pipe$args$lower_thresholds['b']]
        expect_equal(b_where_outside_range, rep(NA_real_, length(b_where_outside_range)))

        b_where_inside_range <- retransformed_with_error$b[new_dataset$b >= r$post_pipe$args$lower_thresholds['b']]
        expect_equal(b_where_inside_range, new_dataset$b[new_dataset$b >= r$post_pipe$args$lower_thresholds['b']])
    })

    it("can deal with NA's while retransforming", {
        retransform_columns <- c("m", "m2")

        r <- pipe_feature_transformer(train = dataset1, response = "x", transform_functions = list(sqrt, log, function(x) x ^ 2, exp),
                                      retransform_columns = retransform_columns)
        retransformed <- invoke(r$post_pipe, r$train)

        for(col in retransform_columns) expect_equal(unlist(retransformed[col]), unlist(dataset1[col]))
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

testthat::describe("pipe_scaler()", {
    r_01 <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "[0-1]")
    r_normal <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "N(0,1)")
    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r_01)
        ctest_pipe_has_correct_fields(r_normal)
    })

    it("scales numeric features to the predefined range", {
        ctest_01_range(r_01$train$a)
        ctest_01_range(r_01$train$b)

        ctest_normal_range(r_normal$train$a)
        ctest_normal_range(r_normal$train$b)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_scaler_predict()", {
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
        expect_error(pipe_scaler(train = dataset1[0, ], exclude_columns = "x", type = "N(0,1)"),
                     regexp = "nrow(train) > 0 is not TRUE", fixed = T)
    })

    it("can create a rescale pipe for requested columns", {
        retransform_columns <- c("a", "b")
        r_01 <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "[0-1]", retransform_columns = retransform_columns)
        r_normal <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "N(0,1)", retransform_columns = retransform_columns)

        expect_true("post_pipe" %in% names(r_01))
        expect_true("post_pipe" %in% names(r_normal))

        r_01_retransformed <- invoke(r_01$post_pipe, r_01$train)
        for(col in retransform_columns) expect_equal(unlist(r_01_retransformed[col]), unlist(dataset1[col]))

        r_norm_retransformed <- invoke(r_normal$post_pipe, r_normal$train)
        for(col in retransform_columns) expect_equal(unlist(r_norm_retransformed[col]), unlist(dataset1[col]))
    })

    it("can apply the rescale pipe for new data", {
        retransform_columns <- c("a", "b")
        r_01 <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "[0-1]", retransform_columns = retransform_columns)
        r_normal <- pipe_scaler(train = dataset1, exclude_columns = "x", type = "N(0,1)", retransform_columns = retransform_columns)

        new_data <- mutate(dataset1, a = a + 10, b = b * -.2)

        expect_true("post_pipe" %in% names(r_01))
        expect_true("post_pipe" %in% names(r_normal))

        r_01_new_data_transformed <- invoke(r_01$pipe, new_data)
        r_01_retransformed <- invoke(r_01$post_pipe, r_01_new_data_transformed)
        for(col in retransform_columns) expect_equal(unlist(r_01_retransformed[col]), unlist(new_data[col]))

        r_01_new_data_transformed <- invoke(r_normal$pipe, new_data)
        r_norm_retransformed <- invoke(r_normal$post_pipe, r_01_new_data_transformed)
        for(col in retransform_columns) expect_equal(unlist(r_norm_retransformed[col]), unlist(new_data[col]))
    })
})

# Skeleton
testthat::describe("pipe_one_hot_encode()", {
    r_pca <- pipe_one_hot_encode(train = dataset1, use_pca = T, columns = c("y", "s"))
    r_non <- pipe_one_hot_encode(train = dataset1, use_pca = F)

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
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
        r_mean <- pipe_one_hot_encode(train = dataset1, use_pca = F, columns = cols,
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
        r_mean <- pipe_one_hot_encode(train = dataset1, use_pca = F, columns = cols,
                                      stat_functions = stats_functions, response = "x")
        ctest_pipe_has_working_predict_function(r_mean, dataset1)
    })

    it("can use mean-encoding with PCA", {
        stats_functions <- list("mean" = mean, "quantile" = function(x) quantile(x = x, .25))
        cols <- c("y", "s")
        r_mean_pca <- pipe_one_hot_encode(train = dataset1, use_pca = T, columns = cols,
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
        r_mean_pca <- pipe_one_hot_encode(train = dataset1, use_pca = T, columns = cols,
                                          stat_functions = stats_functions, response = "x")
        ctest_pipe_has_working_predict_function(r_mean_pca, dataset1)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_one_hot_encode_predict()", {
        ctest_pipe_has_working_predict_function(r_non, dataset1)
        ctest_pipe_has_working_predict_function(r_pca, dataset1)
    })

    it("ignores numeric input", {
        ctest_dataset_has_columns(r_pca$train, c("a", "b", "x"))
        ctest_dataset_has_columns(r_non$train, c("a", "b", "x"))
    })
})

# Skeleton
testthat::describe("pipe_categorical_filter()", {
    r <- pipe_categorical_filter(train = dataset1, response = "x",
                                 insufficient_occurance_marker = "marker", threshold_function = function(data) 5)

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("filters out categorical values that occur not often enough and leaves those that do", {
        expect_equal(dataset1$s, r$train$s)
        expect_equal(dataset1$z, r$train$z)
        expect_equal(r$train$y, rep("marker", N))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for feature_categorical_filter_predict()", {
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

describe("pipe_pca()", {
    default_pca_columns <- c("x", "a", "b", "c")
    r <- ctest_for_no_errors(pipe_pca(train = dataset1, columns = default_pca_columns, pca_tol = .05),
                             error_message = "pipe_pca does not run on basic settings")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("create PCA transformations of the requested columns", {
        expect_false(any(colnames(r$train) %in% default_pca_columns))

        pca_set <- as_data_frame(predict(r$pipe$args$pca, dataset1))
        predicted_set <- r$train[, grepl(pattern = "^PC", x = colnames(r$train))]

        expect_equal(pca_set, predicted_set)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_pca_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("gives a warning when training on missing data", {
        pca_columns_with_missing_values <- c("x", "a", "b", "c", "m2")
        expect_warning(pipe_pca(train = dataset1, columns = pca_columns_with_missing_values, pca_tol = .05),
                       regexp = c("Removed \\d+\\.\\d+\\% of rows due to missing values"))
    })

    it("gives a warning when predicting on missing data", {
        pca_columns_with_missing_values <- c("x", "a", "b", "c", "m2")
        r_with_missing <- suppressWarnings(pipe_pca(train = dataset1, columns = pca_columns_with_missing_values, pca_tol = .05))

        expect_warning(invoke(r_with_missing$pipe, dataset1), regexp = c("Encountered missing values in data to be used for PCA transformation"))
    })

    it("handles missing values", {
        pca_columns_with_missing_values <- c("x", "a", "b", "c", "m2")
        r_with_missing <- suppressWarnings(pipe_pca(train = dataset1, columns = pca_columns_with_missing_values, pca_tol = .05))

        missing_originaly <- apply(dataset1[, pca_columns_with_missing_values], 1 , anyNA)
        pca_set <- r_with_missing$train[grepl(pattern = "PC", x = colnames(r_with_missing$train))]

        expect_equal(nrow(pca_set), length(missing_originaly))
        expect_false(any(!is.na(pca_set[missing_originaly, ])))
        expect_false(any(is.na(pca_set[!missing_originaly, ])))
    })


    it("stops if no data is left to train on after removing missing values", {
        pca_columns_with_missing_values <- c("x", "a", "b", "c", "m2")
        no_good_rows <- filter(dataset1, is.na(m2))

        expect_error(pipe_pca(train = no_good_rows, columns = pca_columns_with_missing_values, pca_tol = .05),
                       regexp = "No rows were left after checking for NA's", fixed = T)
    })

    it("allows you to set a flag to keep the old columns", {
        r_with_old <- pipe_pca(train = dataset1, columns = default_pca_columns, pca_tol = .05, keep_old_columns = T)
        expect_false(any(!default_pca_columns %in% colnames(r_with_old$train)))

        pca_set <- as_data_frame(predict(r_with_old$pipe$args$pca, dataset1))
        predicted_set <- r_with_old$train[, grepl(pattern = "^PC", x = colnames(r_with_old$train))]

        expect_equal(pca_set, predicted_set)
    })
})
