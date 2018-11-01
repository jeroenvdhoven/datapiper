describe("feature_NA_indicators", {
    r <- ctest_for_no_errors(datapiper::feature_NA_indicators(train = dataset1),
                             "Can't apply feature_NA_indicators")

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("can generate NA indicator columns", {
        any_missing_columns <- colnames(dataset1)[purrr::map_lgl(dataset1, anyNA)]
        indicator_columns <- paste0(any_missing_columns, "_NA_indicator")

        ctest_dataset_has_columns(r$train, indicator_columns)
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_NA_indicators_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can handle multiple NA functions", {
        r_multi <- datapiper::feature_NA_indicators(train = dataset1, conditions = list(is.na, function(x) x == 1))
        ctest_dataset_has_columns(dataset = r_multi$train, c("x_NA_indicator", "a_NA_indicator", "c_NA_indicator"))
    })
})


describe("feature_create_all_generic_stats", {
    f_list <- list("mean" = mean, "sd" = sd)
    r <- ctest_for_no_errors(
        datapiper::feature_create_all_generic_stats(
            train = dataset1, response = "x", interaction_level = 1,
            too_few_observations_cutoff = 5, functions = f_list),
        "Can't apply feature_create_all_generic_stats")

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
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

        r_sub <- feature_create_all_generic_stats(
                train = train, response = "x", interaction_level = 1,
                too_few_observations_cutoff = 5, functions = f_list)

        transformed_test <- invoke(r_sub$pipe, test)
        expected_columns <- purrr::map(names(f_list), ~ paste0(., "_", c("y", "s", "z"))) %>% unlist

        expect_false(any(!expected_columns %in% colnames(transformed_test)))
        expect_false(anyNA(transformed_test[, expected_columns]))

        expect_false(any(r_sub$train$mean_y != mean(x = train$x)))
        expect_false(any(transformed_test$mean_y != mean(x = train$x)))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_create_all_generic_stats_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("handles missing values", {
        r_missing <- datapiper::feature_create_all_generic_stats(
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

    it("can go up to interaction level 2", {
        affected_columns <- c("y", "s", "z")

        r_multi <- datapiper::feature_create_all_generic_stats(
            train = dataset1, response = "x", interaction_level = 2,
            too_few_observations_cutoff = 1, functions = f_list)

        expected_columns <- purrr::map(names(f_list), function(f){
            combinations <- expand.grid(affected_columns, affected_columns, stringsAsFactors = F) %>%
                filter(Var1 != Var2) %>%
                apply(1, paste0, collapse = "_")
            return(paste0(f, "_", combinations))
        }) %>% unlist

        expect_equal(sum(expected_columns %in% colnames(r_multi$train)), length(expected_columns) / 2)
    })
})

describe("remove_single_value_columns", {
    r <- ctest_for_no_errors(datapiper::remove_single_value_columns(dataset1, na_function = is.na),
                             error_message = "Error: remove_single_value_columns failed on basic run")

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("removes constant columns", {
        ctest_dataset_has_columns(dataset1, c("z", "z2"))
        ctest_dataset_does_not_have_columns(r$train, c("z", "z2"))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for remove_single_value_columns_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("can ignore missing values", {
        r <- datapiper::remove_single_value_columns(dataset1)
        ctest_dataset_has_columns(dataset1, c("z"))
        ctest_dataset_does_not_have_columns(r$train, c("z"))
        ctest_dataset_has_columns(r$train, c("z2"))
    })
})

describe("feature_interactions", {
    r <- ctest_for_no_errors(datapiper::feature_interactions(dataset1, response = "x", columns = c("a", "b", "c"), max_interactions = 3),
                             error_message = "feature_interactions does not run with defaults")

    it("returns a list with at least train and .predict names, where the first is a dataset and the second a function", {
        ctest_pipe_has_correct_fields(r)
    })

    it("calculates interactions between columns", {
        ctest_dataset_has_columns(dataset = r$train, c("interaction_a_b", "interaction_a_c", "interaction_b_c", "interaction_a_b_c"))
        expect_equal(r$train$interaction_a_b, with(dataset1, (a - mean(a)) * (b - mean(b))))
        expect_equal(r$train$interaction_a_c, with(dataset1, (a - mean(a)) * (c - mean(c))))
        expect_equal(r$train$interaction_b_c, with(dataset1, (b - mean(b)) * (c - mean(c))))
    })

    it("can apply its results to a new dataset using .predict, a wrapper for feature_interactions_predict()", {
        ctest_pipe_has_working_predict_function(r, dataset1)
    })

    it("ignores non-numeric input", {
        r_no_numerics <- datapiper::feature_interactions(dataset1, response = "x", max_interactions = 2, columns = 2)
        expect_false(any(!purrr::map_lgl(dataset1[, r_no_numerics$pipe$args$columns], is.numeric)), info = "All used columns should be numeric")
    })

    it("handles missing values", {
        ctest_for_no_errors(datapiper::feature_interactions(dataset1, response = "x", max_interactions = 2, columns = 2),
                            error_message = "feature_interactions can handle missing values")
    })
})
