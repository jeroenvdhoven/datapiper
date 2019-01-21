#' Standardises column names into an oft-acceptable format
#'
#' @param data The dataset to standardise
#'
#' @return The dataset with standardised column names
#' @export
#'
#' @examples
#' data <- dplyr::data_frame(var = 0, Var = 0, camelCase = 0,
#'                           good_name = 0, `0none.` = 0, `bad  ` = 0,
#'                           `j&d` = 0, `spac ed` = 0)
#' standard_column_names(data)
standard_column_names <- function(data) {
    colnames(data) <- gsub(pattern = ".", replacement = " ", x = colnames(data), fixed = T) %>%
        trimws %>%
        gsub(pattern = " ", replacement = "_", x = ., fixed = T) %>%
        gsub(pattern = "[^a-zA-Z0-9_]", replacement = "_", x = .) %>%
        gsub(pattern = "([^_])([A-Z][a-z0-9])", replacement = "\\1_\\2", x = .) %>%
        gsub(pattern = "^([0-9])", replacement = "n\\1", x = .) %>%
        tolower

    return(data)
}

select_cols <- function(data, cols = colnames(data), rows = seq_len(nrow(data))) {
    if(is.data.table(data)) return(data[rows, cols, with = F])
    else return(select_(data, .dots = cols)[rows, ])
}

deselect_cols <- function(data, cols, inplace = F) {
    if(is.data.table(data)) {
        if(inplace){
            return(data[, c(cols) := NULL])
        } else return(data[, .SD, .SDcols = colnames(data)[!colnames(data) %in% cols]])
    } else return(select_(data, .dots = paste0("-", cols)))
}
