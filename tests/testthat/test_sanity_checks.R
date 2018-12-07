context("Sanity checker")
test_column_value <- function(data, original_column, compare_column, value, test_inclusive = T) {
    if(test_inclusive) {
        filtered_data <- filter(data, column_name %in% original_column)
        txt <- paste0("Column '", paste0(compare_column, collapse = ", "), "' does not equal ", value, "' for column(s) '", paste0(original_column, collapse = ", "), "'")
    } else {
        filtered_data <- filter(data, !column_name %in% original_column)
        txt <- paste0("Column '", paste0(compare_column, collapse = ", "), "' does not equal ", value, "' for column(s) unequal to '", paste0(original_column, collapse = ", "), "'")
    }
    value <- rep(value, nrow(filtered_data))
    expect_equivalent(unlist(filtered_data[compare_column]), value, info = txt)
}

test_full_column_value <- function(data, original_column, compare_column, true_value, false_value) {
    test_column_value(data, original_column, compare_column, true_value, T)
    test_column_value(data, original_column, compare_column, false_value, F)
}

test_read <- function(file) {
    if(!file.exists(file)) file <- paste0("tests/testthat/", file)
    return(scan(file, sep = "\n", what = "char", quiet = T))
}

describe("sanity_checks()", {
    set.seed(1)
    create_formats <- function(sep) {
        return(c(
            paste0("2017", sep, "01", sep, "01"),
            paste0("01", sep, "2017", sep, "01"),
            paste0("01", sep, "01", sep, "2017"),
            paste0("17", sep, "01", sep, "01"),

            paste0("aug", sep, "01", sep, "0123"),
            paste0("aug", sep, "0123", sep, "01"),
            paste0("01", sep, "aug", sep, "0123"),
            paste0("0134", sep, "aug", sep, "01"),
            paste0("0123", sep, "01", sep, "aug"),
            paste0("03", sep, "0134", sep, "aug"),

            paste0("aug", sep, "34", sep, "01"),
            paste0("04", sep, "aug", sep, "01"),
            paste0("04", sep, "05", sep, "aug")
        ))
    }

    people_names <- test_read("dummy_data_for_tests/names_for_tests.txt")
    phone_numbers <- test_read("dummy_data_for_tests/phone_numbers_for_test.txt")
    ips <- test_read("dummy_data_for_tests/ip_for_tests.txt")

    date_formats <- c(
        create_formats("-"),
        create_formats("/"),
        create_formats(".")
    )
    N <- 500
    t <- data_frame(
        a = seq_len(N),
        b = sample(x = c(0,1), size = N, replace = T),
        c = rnorm(n = N),
        d = 1,
        e = "A",
        f = sample(x = c(letters[1:5]), size = N, replace = T),
        g = factor(f),
        h = letters[(a %% 26) + 1],
        i = seq.Date(from = Sys.Date() - N, to = Sys.Date(), length.out = N),
        j = seq.POSIXt(from = Sys.time() - N, to = Sys.time(), length.out = N),
        k = sample(x = c(T,F), size = N, replace = T),
        l = as.character(a),
        m = c(seq_len(N-1), -1e5),
        n = sample(c(1,2,3,4, -1, 0), size = N, replace = T),
        o = sample(c("A", "B", "NA"), size = N, replace = T),
        p = sample(c(4,5,6,7, NaN), size = N, replace = T),
        q = as.character(i),
        r = as.character(j),
        s = sample(date_formats, size = N, replace = T),
        email = purrr::map_chr(seq_len(N), ~ paste0(
            paste0(sample(c(letters, 1:10, "_"), replace = T, size = rbinom(n = 1, size = 100, prob = .5) + 2), collapse = ""),
            "@", paste0(sample(c(letters, 1:10, "_"), replace = T, size = rbinom(n = 1, size = 100, prob = .5) + 2), collapse = ""),
            ".", paste0(collapse = "", sample(letters, replace = T, size = 3)))),
        name = sample(x = people_names, size = N, replace = T),
        address = "",
        phone = sample(x = phone_numbers, size = N, replace = T),
        ip = sample(x = ips, size = N, replace = T)
    )

    numeric_columns <- colnames(t)[purrr::map_lgl(t, ~ is.numeric(.) || is.logical(.))]
    categorical_columns <- colnames(t)[purrr::map_lgl(t, ~ is.character(.) || is.factor(.))]

    r <- ctest_for_no_errors(datapiper::sanity_checking(data = t),
                             error_message = "Can't run sanity_checking")

    it("checks if numeric colums are actually numeric and not categorical", {
        expect_true("numerical_summary" %in% names(r))
        n_num <- r$numerical_summary

        expect_true(is.data.frame(n_num))
        expect_named(n_num, c("column_name", "number_of_unique_values", "ratio_of_unique_values", "lower_bound", "upper_bound", "any_outside"))

        expect_true(is.character(n_num$column_name))
        expect_equal(sort(numeric_columns), sort(n_num$column_name))

        expect_true(is.numeric(n_num$number_of_unique_values))
        expect_false(any(n_num$number_of_unique_values > N))
        expect_false(any(n_num$number_of_unique_values < 1))

        expect_true(is.numeric(n_num$ratio_of_unique_values))
        expect_false(any(n_num$ratio_of_unique_values > 1))
        expect_false(any(n_num$ratio_of_unique_values < 0))

        test_column_value(data = n_num, original_column = c("a", "c", "m"), compare_column = "ratio_of_unique_values", 1)
        test_column_value(data = n_num, original_column = c("a", "c", "m"), compare_column = "number_of_unique_values", N)

        test_column_value(data = n_num, original_column = c("b", "k"), compare_column = "ratio_of_unique_values", 2 / N)
        test_column_value(data = n_num, original_column = c("b", "k"), compare_column = "number_of_unique_values", 2)
    })

    it("checks if character colums are actually character and not numeric", {
        expect_true("categorical_summary" %in% names(r))
        n_cat <- r$categorical_summary

        expect_true(is.data.frame(n_cat))
        expect_named(n_cat, c("column_name", "ratio_numerical", "categorical_outliers"))

        expect_true(is.character(n_cat$column_name))
        expect_equal(sort(categorical_columns), sort(n_cat$column_name))

        expect_true(is.numeric(n_cat$ratio_numerical))
        expect_false(any(n_cat$ratio_numerical > 1))
        expect_false(any(n_cat$ratio_numerical < 0))

        test_full_column_value(data = n_cat, original_column = "l", compare_column = "ratio_numerical", 1, 0)
    })

    it("detects outliers in numeric columns", {
        n_num <- r$numerical_summary
        expect_true(filter(n_num, column_name == "m")$any_outside)
        expect_false(any(is.na(n_num$any_outside)))
        expect_false(any(filter(n_num, column_name %in% c("a", "b", "d", "k", "n", "p"))$any_outside))
    })

    it("detects outliers in character columns", {
        n_cat <- r$categorical_summary
        expect_true(is.list(n_cat$categorical_outliers))
        expect_false(any(purrr::map_lgl(n_cat$categorical_outliers, ~any(!is.character(.)))))
        expect_equal(sort(unlist(filter(n_cat, column_name == "l")$categorical_outliers)), sort(unique(t$l)))
    })

    it("detects possible encodings for missing values per column", {
        expect_true("missing_value_table" %in% names(r))
        missing_table <- r$missing_value_table

        any_missing <- dplyr::filter(missing_table, total_proportion_of_missing_values > 0)
        expect_false("k" %in% any_missing$column_name, info = "Boolean columns shouldn't trigger the '== 0' check")
        expect_false(any(!c("b", "n", "o", "p") %in% any_missing$column_name))

        p_row <- filter(missing_table, column_name == "p")
        expect_false(p_row$`nsv:NA` == p_row$`nsv:NaN`, info = "Make sure that NaN doesn't get counted for NA values")
        expect_gt(p_row$`nsv:NaN`, 0)
        expect_lte(p_row$`nsv:NaN`, 1)
    })

    it("detects encodings of different types of variables", {
        expect_true("encoding_table" %in% names(r))
        encoding_table <- r$encoding_table

        expect_false(any(!c("column_name", "email", "date", "timestamp") %in% colnames(encoding_table)))

        expect_true(is.character(encoding_table$column_name))
        expect_equal(sort(categorical_columns), sort(encoding_table$column_name))

        test_full_column_value(encoding_table, "r", "timestamp", 1, 0)
        test_full_column_value(encoding_table, "email", "email", 1, 0)
        test_full_column_value(encoding_table, "name", "name", 1, 0)
        test_full_column_value(encoding_table, "ip", "ip", 1, 0)

        test_column_value(encoding_table, c("s", "r", "q"), "date", 1, test_inclusive = T)
        expect_false(any(filter(encoding_table, column_name != "phone")$phone_number > .2), info = "Not too many false positives for address")

        test_column_value(encoding_table, "phone", "phone_number", 1, test_inclusive = T)
        expect_false(any(filter(encoding_table, column_name != "phone")$phone_number > .05), info = "Not too many false positives for phone numbers")
    })

    it("allows you to check for other regexes as well", {
        r_extra <- sanity_checking(data = t, other_type_regexs = c("has_numbers" = "\\d+"))
        encodings <- r_extra$encoding_table

        expect_true("has_numbers" %in% colnames(encodings))
    })
})

