#' Add features one-by-one to find good small sets of features
#'
#' @param train The training dataset
#' @param test The testing dataset
#' @param response The response column as a string
#' @param model A list containing at least a training function \code{.train} and a \code{.predict} function, plus optional named
#' parameters to the train function.
#'
#' The \code{.train} function has to take a \code{data} argument that stores the training data and a \code{...} argument for the parameters.
#' The \code{.predict} function needs to take two arguments, where the first is the model and the second the new dataset.
#'
#' You can use \code{\link{model_trainer}} as a wrapper for this list. It will also test your inputs to a certain degree.
#' @param difference A function that calculates the difference between labels and predictions, for example
#' \code{function(x,y) abs(x-y)}
#' @param verbose Flag indicating if intermediate updates should be printed
#' @param n_features The number of features to try before stopping the algorithm. Can be passed as a integer between 1 and \code{ncol(train)} or a
#' fraction, in which case the number of features to try will be \code{n_features * ncol(train)}.
#' Defaults to \code{ncol(train)}, only taking numeric columns into account.
#'
#' @details This function will select features one-by-one depending on the correlation of a feature with the residuals of the train predictions to slowly build up a bigger model.
#'
#' @return A dataframe, containing the mean difference for train and test set, as well as the feature that was added at each step.
#' @export
feature_finder <- function(train, test, response, model, difference, n_features, verbose = F) {
    stopifnot(
        !missing(train), is.data.frame(train),
        !missing(test) , is.data.frame(test),
        is.character(response), length(response) == 1,
        response %in% colnames(train),
        response %in% colnames(test),
        is.list(model), c(".train", ".predict") %in% names(model),
        is.function(model$.train), is.function(model$.predict),
        is.function(difference), length(formalArgs(difference)) >= 2
    )

    numerics_train <- purrr::map_lgl(.x = train, is.numeric)
    if(any(!numerics_train)) {
        if(verbose) warning("Warning: found non-numerics in the train set. Continuing with only numeric columns")
        train <- train[, numerics_train]
    }

    if(missing(n_features)) n_features <- ncol(train)
    else {
        stopifnot(
            is.numeric(n_features),
            length(n_features) == 1,
            n_features <= ncol(train),
            n_features >= 0
        )
        if(n_features < 1) n_features <- n_features * ncol(train)
        n_features <- round(n_features)
    }

    numerics_test <- purrr::map_lgl(.x = test, is.numeric)
    if(any(!numerics_test)) {
        if(verbose) warning("Warning: found non-numerics in the test set. Continuing with only numeric columns")
        test <- test[, numerics_test]
    }

    stopifnot(
        !any(!colnames(train) %in% colnames(test)),
        !any(!colnames(test) %in% colnames(train))
    )

    train_response <- unlist(train[response])
    test_response <- unlist(test[response])

    columns <- character(length = 0)
    correlations <- purrr::map_dbl(train, cor, y = train_response, use = "complete.obs")
    result <- dplyr::data_frame(train = 0, test = 0, column = "")[0,]

    model_args <- model[!names(model) %in% c(".train", ".predict")]
    for(i in seq_len(n_features)) {
        valid_pos <- colnames(train) != response & !colnames(train) %in% columns
        if(any(valid_pos)){

            best_col <- colnames(train)[valid_pos & abs(correlations[]) == max(abs(correlations[valid_pos]), na.rm = T)][1]
            if(best_col %in% columns) {
                warning("Error: tried to add a duplicate column to the list of best column")
                return(result)
            }
            columns <- c(columns, best_col)

            train_subset <- train[, c(response, columns)]
            test_subset <- test[, c(response, columns)]

            model_args$data <- train_subset
            m <- do.call(what = model$.train, args = model_args)

            residuals <- difference(model$.predict(m, train_subset), train_response)
            train_error <- difference(model$.predict(m, train_subset), train_response)
            test_error <- difference(model$.predict(m, test_subset), test_response)

            correlations <- purrr::map_dbl(train, cor, y = residuals, use = "complete.obs")

            tmp_res <- dplyr::data_frame(train = mean(train_error, na.rm = T), test = mean(test_error, na.rm = T), column = best_col)
            result[i, ] <- tmp_res

            # if(!any(abs(correlations) > .3)) break
            if(any(is.nan(test_error))){
                if(verbose) message("train error: ", mean(train_error, na.rm = T), "\ttest error: ", mean(test_error, na.rm = T), "\tselected:", best_col,
                    "\terrors:", sum(is.na(test_error) | is.nan(test_error)), sep = "")
            } else {
                if(verbose) message("train error: ", mean(train_error, na.rm = T), "\ttest error: ", mean(test_error, na.rm = T), "\tselected:", best_col, sep = "")
            }
        }
    }

    return(result)
}
