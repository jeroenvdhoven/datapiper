#' Checks for a couple of standard things to ensure the data is ready for use.
#'
#' @param data The dataset to test
#' @param missing_values A list of missing values to test for. Strings will only be tested against character columns, NA, NaN, NULL and numerical values against all columns.
#' @param min_character_freq How often an entry in a character column needs to occur to not be marked as an "outlier".
#' @param n_sigma The n in \code{[median - n*sd, median + n*sd]}, used to determine upper and lower bounds on acceptable values for numerical columns
#' @param other_type_regexs Additional regexes to check for possible encoding of other variables
#'
#' @return A list with 3 dataframes:
#' \itemize{
#' \item numerical_summary
#' \item categorical_summary
#' \item missing_value_table
#' }
#'
#' @details
#' Test the following items:
#' \itemize{
#' \item Column types are correct, ergo detect if:
#'   \itemize{
#'   \item Character columns don't contain to many numbers. (Bad interpretation of a column)
#'   \item Numeric columns actually represent numerical scales, e.g. no categorical encoding.
#'   }
#' \item (Simple) outlier detection per column. This will not take into account the above mentioned column checks:
#'   \itemize{
#'   \item Character columns: occurances of entries < \code{min_character_freq} and which these are.
#'   \item Numeric columns: occurances of values outside the range \code{[median - n*sd, median + n*sd]}. The bounds will be set within the range of each column.
#'   Will give this range and a flag indicating if there are any records outside of this range
#'   }
#' \item Detect possible encodings of missing values:
#'   \itemize{
#'   \item All columns: 0, -1, NA, NULL, NaN
#'   \item Character columns: "NA", "none", "<NA>", "NULL", "NaN", ""
#'   }
#'   Non-character/numeric values such as NA, NaN and NULL will get \code{nsv:} prepended to them (non-standard value)
#' \item Detect possible encodings for other date types in character columns, such as:
#'   \itemize{
#'   \item Dates
#'   \item Timestamps
#'   \item Emails
#'   }
#' }
#' @importFrom purrr map_lgl map map_int map_dfr map_dfc map_dbl
#' @importFrom dplyr data_frame
#' @importFrom stats median sd
#' @export
sanity_checking <- function(data,
                            missing_values = list(NA, "NA", "<NA>", "none", "", NULL, "NULL", "NaN", NaN, 0, -1),
                            min_character_freq = ceiling(log(nrow(data), base = 2)),
                            other_type_regexs = character(0),
                            n_sigma = 3
) {
    stopifnot(
        is.data.frame(data), nrow(data) > 0, ncol(data) > 0,
        is.character(other_type_regexs)
    )
    if(length(other_type_regexs) > 0 & is.null(names(other_type_regexs)))
        names(other_type_regexs) <- paste0("regex_", seq_along(other_type_regexs))

    numeric_cols <- purrr::map_lgl(data, ~ is.numeric(.) || is.logical(.))
    char_cols <- purrr::map_lgl(data, ~ is.character(.) || is.factor(.))

    result <- list()

    # analyse numeric columns
    num_data <- data[, numeric_cols, drop = F]
    result$numerical_summary <- check_numerical(num_data, n_sigma)

    # analyse character columns
    char_data <- data[, char_cols, drop = F]
    result$categorical_summary <- check_categorical(char_data, min_character_freq)

    # create encoding table
    result$encoding_table <- check_encodings(categorical_data = char_data, other_type_regexs = other_type_regexs)

    # create missing value table
    result$missing_value_table <- find_missing_values(data = data, missing_values = missing_values)


    return(result)
}

check_numerical <- function(numerical_data, n_sigma) {
    num_uniques <- purrr::map(numerical_data, function(x) {
        x <- unique(x)
        return(x[!is.na(x)])
    })
    no_uniques <- purrr::map_int(num_uniques, length)

    outlier_ranges <- purrr::map_dfr(numerical_data, function(x){
        m <- stats::median(x = x, na.rm = T)
        s <- stats::sd(x = x, na.rm = T)
        lower = max(m - s * n_sigma, min(x, na.rm = T), na.rm = T)
        upper = min(m + s * n_sigma, max(x, na.rm = T), na.rm = T)
        return(list(
            lower = lower,
            upper = upper,
            any_outside = any(x > upper | x < lower, na.rm = T)
        ))
    })

    return(data_frame(
        column_name = colnames(numerical_data),
        number_of_unique_values = no_uniques,
        ratio_of_unique_values = no_uniques / nrow(numerical_data),
        lower_bound = outlier_ranges$lower,
        upper_bound = outlier_ranges$upper,
        any_outside = outlier_ranges$any_outside
    ))
}

check_categorical <- function(categorical_data, min_character_freq) {
    fraction_pure_numerical <- purrr::map_dbl(categorical_data, function(x) {
        x <- x[!is.na(x)]
        is_numerical <- grepl(x = x, pattern = "^[0-9]+((\\.|,)[0-9]+)?$")
        return(sum(is_numerical, na.rm = T) / length(is_numerical))
    })

    categorical_outliers <- purrr::map(categorical_data, function(x, min_freq){
        tabled_x <- table(x)
        outliers <- names(tabled_x)[tabled_x <= min_freq]
    }, min_freq = min_character_freq)

    return(data_frame(
        column_name = colnames(categorical_data),
        ratio_numerical = fraction_pure_numerical,
        categorical_outliers = categorical_outliers
    ))
}

find_missing_values <- function(data, missing_values) {
    missing_value_names <- missing_values
    missing_value_names[!purrr::map_lgl(missing_value_names, ~ (is.numeric(.) && !is.nan(.)) || is.character(.))] %<>% paste0("nsv:", .)
    missing_value_names[missing_value_names == ""] <- "empty_string"
    names(missing_values) <- missing_value_names
    missing_value_table <- purrr::map_dfr(.x = data, .f = function(column){
        purrr::map_dfc(.x = missing_values, .f = function(value, column){
            result <- 0
            if(is.character(value)){
                if(is.character(column)) result <- sum(value == column, na.rm = T)
            } else if(is.numeric(value) && !is.nan(value)) {
                if(is.logical(column)) result <- 0
                else result <- sum(value == column, na.rm = T)
            } else if(is.null(value)) {
                result <- sum(is.null(column), na.rm = T)
            } else if(is.na(value) && !is.nan(value)) {
                result <- sum(is.na(column) && !is.nan(value), na.rm = T)
            } else if(is.nan(value)) {
                result <- sum(is.nan(column), na.rm = T)
            }
            return(result / length(column))
        }, column = column)
    })
    missing_value_table$total_proportion_of_missing_values <- apply(missing_value_table, 1, sum)
    missing_value_table <- bind_cols("column_name" = colnames(data), missing_value_table)

    return(missing_value_table)
}

check_encodings <- function(categorical_data, other_type_regexs = character(0)) {
    # 00/00/00, with the year being possibly anywhere
    # sep characters: . / -
    # 00/Jan/00, with the month being any possible size
    date_sep <- "([/\\.]|-)"
    full_month <- "([a-zA-Z]+)"
    short_month <- "([0-9]{2}([0-9]{2})?)"
    date_checker <- paste0(
        "([^/\\.]|^)(",
        "(", full_month, date_sep, short_month, date_sep, short_month, ")|",
        "(", short_month, date_sep, full_month, date_sep, short_month, ")|",
        "(", short_month, date_sep, short_month, date_sep, full_month, ")|",
        "(", short_month, date_sep, short_month, date_sep, short_month, ")",
        ")([^/\\.]|$)"
    )

    ipv6_block <- "([0-9A-Fa-f]{1,4})"
    mid_block <- paste0("(:", ipv6_block, ")")
    ipv4 <- "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}"
    ipv6 <- paste0(ipv6_block, "(", mid_block, "*::)|(", mid_block, "{5})")
    phone_number_start <- "((\\+[0-9]+)?(\\([0-9]+\\))?( |-)?)" #may start with a plus and numbers, followed by ([0-9]). Can end in ? or space
    phone_number_main <- "(([0-9]{6}[0-9]+)|(([0-9]{2}[0-9]+)(( |-)([0-9]{2}[0-9]+))+))" #either at least 6 digits following each other, or separated by at least 1 separator

    base_types <- c(
        "email" = "([^@]+)@([^@\\.]+)\\.([a-z])+",
        "date" = date_checker,
        "name" = "[A-Z][a-z]+(([ ']|-)([A-Z][a-z]+))*",
        "phone_number" = paste0(phone_number_start, phone_number_main),
        # "address" = "",
        # "bsn" = "",
        # "zipcode" = "[0-9A-Za-z]{1-6}( [0-9A-Za-z]{1-6})?",
        "ip" = paste0("(", ipv4, ")|(", ipv6, ")"),
        "timestamp" = "(([0-1][0-9])|(2[0-4])):[0-5][0-9]:[0-5][0-9]"
    )
    regexes <- c(base_types, other_type_regexs)

    encoding_table <- purrr::map_dfr(.x = categorical_data, .f = function(column){
        purrr::map_dfc(.x = regexes, .f = function(regex, column){
            mean(grepl(pattern = regex, x = column), na.rm = T)
        }, column = column)
    })
    encoding_table <- bind_cols("column_name" = colnames(categorical_data), encoding_table)

    return(encoding_table)
}
