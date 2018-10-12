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