row_reduction <- function(data, response, threshold = .2) {
    response_col <- unlist(data[, response])
    data <- select_(data, paste0("-", response))
    columns_are_numeric <- purrr::map_lgl(.x = data, function(x) is.numeric(x) || is.logical(x))
    num_ranges <- purrr::map_dbl(.x = data[, columns_are_numeric], function(x) max(x, na.rm = T) - min(x, na.rm = T))

    if(any(!columns_are_numeric)) {
        warning("Warning: Only numeric columns allowed. Will remove non-numeric columns\n")
        data <- data[, columns_are_numeric]
    }

    metric <- function(dataset, row) {
        differences <- purrr::pmap_df(.l = list(x = dataset, y = row, num_range = num_ranges), .f = function(x, y, num_range) abs(x - y) / num_range) %>%
            apply(MARGIN = 1, FUN = sum, na.rm = T)
        return(differences / ncol(row))
    }

    representative_points <- data[1,]
    responses_per_point <- list(response_col[1])
    weights <- 1
    representative_indices <- 1
    for(i in seq_len(nrow(data) - 1) + 1) {
        current_row <- data[i,]
        distances <- metric(dataset = representative_points, row = current_row)

        stopifnot(length(distances) > 0)
        if(any(distances <= threshold)){
            # Found a cluster close enough to current point: assign it.
            index <- which.min(distances)
            weights[index] <- weights[index] + 1
            responses_per_point[[index]] <- c(responses_per_point[[index]], response_col[i])
        } else {
            # No cluster found that's close enough: create new cluster
            new_position <- nrow(representative_points) + 1
            representative_points[new_position,] <- current_row
            responses_per_point[[new_position]] <- response_col[i]
            weights[new_position] <- 1
            representative_indices[new_position] <- i
        }
    }

    weight_vector <- numeric(length = nrow(data))
    weight_vector[representative_indices] <- weights

    return(list(
        weights = weight_vector,
        points = representative_points,
        responses = responses_per_point
    ))
}