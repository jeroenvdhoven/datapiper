context("Utils")
describe("standard_column_names", {
    ctest_column_name_transformation <- function(starting_names, expected_names) {
        cols <- as.list(seq_along(starting_names))
        names(cols) <- starting_names

        data <- do.call(dplyr::data_frame, cols)

        transformed <- standard_column_names(data)
        expect_named(transformed, expected_names)
    }

    it("standardised camelcase", {
        ctest_column_name_transformation("camelCase", "camel_case")
    })

    it("standardised capitilization", {
        ctest_column_name_transformation("Var", "var")
    })

    it("leaves good names untouched", {
        ctest_column_name_transformation(c("good_name", "var"), c("good_name", "var"))
    })

    it("puts an n in front of names starting with numbers", {
        ctest_column_name_transformation("0none", "n0none")
    })

    it("removes non-alphanumeric characters at the start and end of names except _", {
        ctest_column_name_transformation(c("none.", "var_"), c("none", "var_"))
    })

    it("replaces non-alphanumeric characters in the middle of names with _", {
        ctest_column_name_transformation("j&d", "j_d")
    })

    it("substitutes spaces with _", {
        ctest_column_name_transformation("spa ced", "spa_ced")
    })

    it("substitutes multipe _ by a single _", {
        ctest_column_name_transformation("spa___ced", "spa_ced")
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
