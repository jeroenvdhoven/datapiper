describe("feature_finder()", {
    train_indices <- seq_len(nrow(dataset1) / 2)
    train <- dataset1[train_indices,]
    test <- dataset1[-train_indices,]
    response <- "x"

    model <- find_template_formula_and_data(response = response, training_function = lm)
    difference_function <- function(x,y) abs(x - y)
    numeric_cols <- purrr::map_lgl(.x = dataset1, .f = is.numeric)

    train_numeric <- train[, numeric_cols]
    test_numeric <- test[, numeric_cols]

    it("generates a dataframe with `train, test and column` columns and does not use columns twice", {
        train_no_na <- train_numeric[!apply(train_numeric, 1, anyNA), ]
        test_no_na <- test_numeric[!apply(test_numeric, 1, anyNA), ]
        r <- feature_finder(train = train_no_na, test = test_no_na, response = "x", model = model, difference = difference_function)

        expect_named(r, c("train", "test", "column"))
        expect_equal(nrow(r), ncol(train_no_na) - 1)
    })

    it("can deal with NAs", {
        r <- feature_finder(train = train_numeric, test = test_numeric, response = "x", model = model, difference = difference_function)

        expect_named(r, c("train", "test", "column"))
        expect_equal(nrow(r), ncol(train_numeric) - 1)
    })

    it("uses the best predicting columns first", {
        r <- feature_finder(train = train_numeric, test = test_numeric, response = "x", model = model, difference = difference_function)

        correlations <- sort(purrr::map_dbl(.x = select(train_numeric, -x), cor, y = train_numeric$x), decreasing = T)
        expect_equal(r$column[1], names(correlations[1]))
    })

    it("prints updates when verbose is set to true", {
        expect_message(feature_finder(train = train_numeric, test = test_numeric, response = "x", model = model, difference = difference_function, verbose = T),
                       regexp = "train error: ")
    })

    it("gives a warning when non-numeric columns are used and verbose is true", {
        suppressMessages(expect_warning(feature_finder(train = train, test = test, response = "x", model = model, difference = difference_function, verbose = T),
                            regexp = "found non-numerics in the train set"))

        r <- feature_finder(train = train, test = test, response = "x", model = model, difference = difference_function, verbose = F)

        expect_named(r, c("train", "test", "column"))
        expect_equal(nrow(r), ncol(train_numeric) - 1)
    })
})
