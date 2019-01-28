context("Utils")
describe("standard_column_names", {
    it("standardised camelcase", {
        data <- dplyr::data_frame(camelCase = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, "camel_case")
    })

    it("standardised capitilization", {
        data <- dplyr::data_frame(Var = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, "var")
    })

    it("leaves good names untouched", {
        data <- dplyr::data_frame(good_name = 0, var = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, c("good_name", "var"))
    })

    it("puts an n in front of names starting with numbers", {
        data <- dplyr::data_frame(`0none` = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, c("n0none"))
    })

    it("removes non-alphanumeric characters at the start and end of names except _", {
        data <- dplyr::data_frame(`none.` = 0, var_ = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, c("none", "var_"))
    })

    it("replaces non-alphanumeric characters in the middle of names with _", {
        data <- dplyr::data_frame(`j&d` = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, c("j_d"))
    })

    it("substitutes spaces with _", {
        data <- dplyr::data_frame(`spa ced` = 0)
        transformed <- standard_column_names(data)
        expect_named(transformed, c("spa_ced"))
    })
})


describe("select_cols", {
    it("will select columns for both data.tables and data.frames in the same way", {
        cols <- c("x", "y")

        r_df <- select_cols(data = dataset1, cols = cols)
        r_dt <- as_data_frame(select_cols(data = as.data.table(dataset1), cols = cols))

        expect_equal(expected = cols, colnames(r_df))
        expect_equal(expected = cols, colnames(r_dt))
        expect_equal(expected = nrow(r_df), nrow(dataset1))
        expect_equal(expected = nrow(r_dt), nrow(dataset1))
        expect_equal(r_dt, r_df)
    })

    it("will subset rows for both data.tables and data.frames in the same way", {
        rows <- seq_len(N / 2)

        r_df <- select_cols(data = dataset1, rows = rows)
        r_dt <- as_data_frame(select_cols(data = as.data.table(dataset1), rows = rows))

        expect_equal(expected = colnames(dataset1), colnames(r_df))
        expect_equal(expected = colnames(dataset1), colnames(r_dt))
        expect_equal(expected = length(rows), nrow(r_df))
        expect_equal(expected = length(rows), nrow(r_dt))
        expect_equal(r_dt, r_df)
    })

    it("will subset rows and columns for both data.tables and data.frames in the same way", {
        cols <- c("x", "y")
        rows <- seq_len(N / 2)

        r_df <- select_cols(data = dataset1, rows = rows, cols = cols)
        r_dt <- as_data_frame(select_cols(data = as.data.table(dataset1), rows = rows, cols = cols))

        expect_equal(expected = cols, colnames(r_df))
        expect_equal(expected = cols, colnames(r_dt))
        expect_equal(expected = length(rows), nrow(r_df))
        expect_equal(expected = length(rows), nrow(r_dt))
        expect_equal(r_dt, r_df)
    })
})

describe("deselect_cols", {
    it("can remove columns from both data.tables and data.frames in the same way", {
        cols <- c("x", "y")

        r_df <- deselect_cols(data = dataset1, cols = cols)
        r_dt <- as_data_frame(deselect_cols(data = as.data.table(dataset1), cols = cols, inplace = F))

        expected_columns <- colnames(dataset1)[!colnames(dataset1) %in% cols]
        expect_equal(expected = expected_columns, colnames(r_df))
        expect_equal(expected = expected_columns, colnames(r_dt))
        expect_equal(expected = nrow(dataset1), nrow(r_df))
        expect_equal(expected = nrow(dataset1), nrow(r_dt))
        expect_equal(r_dt, r_df)
    })

    it("can remove columns from data.tables by reference", {
        cols <- c("x", "y")

        tmp <- as.data.table(dataset1)
        r_dt <- deselect_cols(data = tmp, cols = cols, inplace = T)

        expected_columns <- colnames(dataset1)[!colnames(dataset1) %in% cols]
        expect_equal(expected = expected_columns, colnames(r_dt))
        expect_equal(expected = nrow(dataset1), nrow(r_dt))
        expect_equal(r_dt, tmp)
    })
})
