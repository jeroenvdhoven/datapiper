context("Feature generation")
describe("pipe_NA_indicators", {
    r <- ctest_for_no_errors(datapiper::pipe_NA_indicators(train = dataset1),
                             "Can't apply pipe_NA_indicators with default settings")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("can generate NA indicator columns", {
        any_missing_columns <- colnames(dataset1)[purrr::map_lgl(dataset1, anyNA)]
        indicator_columns <- paste0(any_missing_columns, "_NA_indicator")

        ctest_dataset_has_columns(r$train, indicator_columns)
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_NA_indicators_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can handle custom NA functions", {
        na_func <- function(x) x == 1 | is.na(x)
        columns <- c("x", "a", "c")

        r_multi <- datapiper::pipe_NA_indicators(train = dataset1, condition = na_func, columns = columns)
        ctest_dataset_has_columns(dataset = r_multi$train, c("x_NA_indicator", "a_NA_indicator", "c_NA_indicator"))

        for(col in columns) {
            y_expected <- na_func(dataset1[, col])
            expect_equal(expected = y_expected[,1], object = unlist(r_multi$train[, paste0(col, "_NA_indicator")], use.names = F),
                         info = paste(col, "was not equal to its expected value after applying NA function"))
        }
    })

    it("can force columns to exist", {
        col_without_missing_values <- "b"
        r_unforced <- datapiper::pipe_NA_indicators(train = dataset1, condition = is.na, columns = col_without_missing_values, force_column = F)
        r_forced <- datapiper::pipe_NA_indicators(train = dataset1, condition = is.na, columns = col_without_missing_values, force_column = T)

        expect_false("b_NA_indicator" %in% colnames(r_unforced$train))
        expect_true("b_NA_indicator" %in% colnames(r_forced$train))
    })


})


describe("pipe_create_stats", {
    f_list <- list("mean" = mean, "sd" = sd)
    r <- ctest_for_no_errors(
        datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 1,
            too_few_observations_cutoff = 5, functions = f_list),
        "Can't apply pipe_create_stats")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("generates statistics for given groups", {
        expected_columns <- purrr::map(names(f_list), ~ paste0(., "_", c("y", "s", "z"))) %>% unlist
        ctest_dataset_has_columns(r$train, expected_columns)

        # Because of the too_few_observations_cutoff, we use the sample mean and sd
        expect_equal(r$train$mean_y, rep(mean(dataset1$x), nrow(r$train)))
        expect_equal(r$train$sd_y, rep(sd(dataset1$x), nrow(r$train)))

        for(col in c("s", "z")){
            for(i in seq_along(f_list)){
                f <- f_list[[i]]
                stats <- group_by_(.data = dataset1, col) %>%
                    summarise(value = f(x)) %>%
                    arrange_(col)

                stat_col <- paste0(names(f_list)[i], "_", col)
                generated_stats <- group_by_(r$train, col) %>%
                    summarize_(value = paste0(stat_col, "[1]")) %>%
                    arrange_(col)

                expect_identical(stats, generated_stats, info = paste("Is column", col, "generated correctly using function", names(f_list)[i]))
            }
        }
    })

    it("generates default values for categories that don't exist in the train dataset and too-few-oberservation categories based on the entire population", {
        train_indices <- seq_len(nrow(dataset1) / 2)
        train <- dataset1[train_indices,]
        test <- dataset1[-train_indices,]

        r_sub <- pipe_create_stats(
            train = train, response = "x", interaction_level = 1,
            too_few_observations_cutoff = 5, functions = f_list)

        transformed_test <- invoke(r_sub$pipe, test)
        expected_columns <- purrr::map(names(f_list), ~ paste0(., "_", c("y", "s", "z"))) %>% unlist

        expect_false(any(!expected_columns %in% colnames(transformed_test)))
        expect_false(anyNA(transformed_test[, expected_columns]))

        expect_false(any(r_sub$train$mean_y != mean(x = train$x)))
        expect_false(any(transformed_test$mean_y != mean(x = train$x)))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_create_stats_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("handles missing values", {
        r_missing <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 1, stat_cols = "m",
            too_few_observations_cutoff = 1, functions = f_list)

        for(col in c("m")){
            for(i in seq_along(f_list)){
                f <- f_list[[i]]
                stats <- group_by_(.data = dataset1, col) %>%
                    summarise(value = f(x)) %>%
                    arrange_(col)

                stat_col <- paste0(names(f_list)[i], "_", col)
                generated_stats <- group_by_(r_missing$train, col) %>%
                    summarize_(value = paste0(stat_col, "[1]")) %>%
                    arrange_(col)

                expect_identical(stats, generated_stats, info = paste("Is column", col, "generated correctly using function", names(f_list)[i]))
            }
        }
    })

    it("can set different thresholds for what is considered a value that occurs too little", {
        r_no_missing <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 1, stat_cols = "m",
            too_few_observations_cutoff = 0, functions = f_list)
        expect_equal(object = nrow(r_no_missing$pipe$args$tables[[1]]), expected = length(unique(dataset1$m)))

        r_all_missing <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 1, stat_cols = "m",
            too_few_observations_cutoff = nrow(dataset1), functions = f_list)
        expect_equal(object = nrow(r_all_missing$pipe$args$tables[[1]]), expected = 0)
    })

    it("can increase the interaction level to 2", {
        affected_columns <- c("y", "s", "z")

        r_2 <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 2,
            too_few_observations_cutoff = 1, functions = f_list)

        expected_columns <- purrr::map(names(f_list), function(f){
            purrr::map(seq_len(2), function(x) {
                paste(f, sep = "_",
                      apply(combn(x = affected_columns, m = x), FUN = paste, MARGIN = 2, collapse = "_")
                )
            })
        }) %>% unlist
        expect_false(object = any(!expected_columns %in% colnames(r_2$train)))
    })

    it("can increase the interaction level above 2", {
        affected_columns <- c("y", "s", "z")

        r_3 <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 3,
            too_few_observations_cutoff = 1, functions = f_list)

        expected_columns <- purrr::map(names(f_list), function(f){
            purrr::map(seq_len(3), function(x) {
                paste(f, sep = "_",
                      apply(combn(x = affected_columns, m = x), FUN = paste, MARGIN = 2, collapse = "_")
                )
            })
        }) %>% unlist
        expect_false(object = any(!expected_columns %in% colnames(r_3$train)))
    })

    it("can keep the result as a data.table if the input is a data.table", {
        dt_dataset1 <- data.table::as.data.table(dataset1)
        r_dt <- datapiper::pipe_create_stats(
            train = dt_dataset1, response = "x", interaction_level = 2,
            too_few_observations_cutoff = 1, functions = f_list)

        r_df <- datapiper::pipe_create_stats(
            train = dataset1, response = "x", interaction_level = 2,
            too_few_observations_cutoff = 1, functions = f_list)

        expect_true(is.data.table(r_dt$train))
        expect_equal(object = as_data_frame(r_dt$train), expected = r_df$train)
    })
})

describe("pipe_remove_single_value_columns", {
    r <- ctest_for_no_errors(datapiper::pipe_remove_single_value_columns(dataset1, na_function = is.na),
                             error_message = "Error: pipe_remove_single_value_columns failed on basic run")

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("removes constant columns", {
        ctest_dataset_has_columns(dataset1, c("z", "z2"))
        ctest_dataset_does_not_have_columns(r$train, c("z", "z2"))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for pipe_remove_single_value_columns_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can ignore missing values", {
        r <- datapiper::pipe_remove_single_value_columns(dataset1)
        ctest_dataset_has_columns(dataset1, c("z"))
        ctest_dataset_does_not_have_columns(r$train, c("z"))
        ctest_dataset_has_columns(r$train, c("z2"))
    })

    it("can set a custom na function", {
        na_func <- function(x) x < 10 | is.na(x)
        r <- datapiper::pipe_remove_single_value_columns(dataset1, na_function = na_func)

        good_columns <- purrr::map_lgl(.x = dataset1, .f = ~ length(unique(.[!na_func(.)])) > 1L)
        expect_equal(dataset1[, good_columns], r$train)
    })
})

describe("pipe_feature_interactions", {
    r <- ctest_for_no_errors(datapiper::pipe_feature_interactions(dataset1, response = "x", columns = c("a", "b", "c"), max_interactions = 3),
                             error_message = "pipe_feature_interactions does not run with defaults")

    ctest_interactions <- function(data, interactions, n) {
        number_of_uniques <- purrr::map_int(.x = select(dataset1, -x), .f = ~ length(unique(.)))
        number_of_uniques <- number_of_uniques[!purrr::map_lgl(.x = select(dataset1, -x), is.character)]

        r <- datapiper::pipe_feature_interactions(dataset1, response = "x", max_interactions = interactions, columns = n)
        n_generated_columns <- length(number_of_uniques[number_of_uniques > n])

        number_of_interaction_features <- length(colnames(r$train)[grepl(pattern = "^interaction_", x = colnames(r$train))])
        expected_number_of_interaction_features <- sum(choose(n = n_generated_columns, k = seq.int(from = 2, to = interactions)))

        expect_equal(expected = expected_number_of_interaction_features, object = number_of_interaction_features,
                     info = paste(interactions, "interactions and", n, "column level generates not the expected number of columns"))
    }

    it("returns a list with at least train and pipe names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("calculates interactions between columns", {
        ctest_dataset_has_columns(dataset = r$train, c("interaction_a_b", "interaction_a_c", "interaction_b_c", "interaction_a_b_c"))
        expect_equal(r$train$interaction_a_b, with(dataset1, (a - mean(a)) * (b - mean(b))))
        expect_equal(r$train$interaction_a_c, with(dataset1, (a - mean(a)) * (c - mean(c))))
        expect_equal(r$train$interaction_b_c, with(dataset1, (b - mean(b)) * (c - mean(c))))
    })

    it("can apply its results to a new dataset using pipe, a wrapper for feature_interactions_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("ignores non-numeric input", {
        r_no_numerics <- datapiper::pipe_feature_interactions(dataset1, response = "x", max_interactions = 2, columns = 2)
        expect_false(any(!purrr::map_lgl(dataset1[, r_no_numerics$pipe$args$columns], is.numeric)), info = "All used columns should be numeric")
    })

    it("can select columns based on the number of unique values", {
        ctest_interactions(data = dataset1, interactions = 2, n = 5)
        ctest_interactions(data = dataset1, interactions = 2, n = 2)
        ctest_interactions(data = dataset1, interactions = 2, n = 20)
    })

    it("can increase the interaction level above 2", {
        ctest_interactions(data = dataset1, interactions = 2, n = 5)
        ctest_interactions(data = dataset1, interactions = 3, n = 5)
        ctest_interactions(data = dataset1, interactions = 4, n = 5)
        ctest_interactions(data = dataset1, interactions = 5, n = 5)
    })

    it("handles missing values", {
        ctest_for_no_errors(datapiper::pipe_feature_interactions(dataset1, response = "x", max_interactions = 2, columns = 2),
                            error_message = "pipe_feature_interactions can handle missing values")
    })
})
