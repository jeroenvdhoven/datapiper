#' Add cluster labels to a training set
#'
#' @param train The train dataset, as a data.frame.
#' @param cluster_column Name for the new cluster label column
#' @param exclude_columns Columns to be excluded from the clustering. Should be provided as a character vector.
#' Non-numerical columns will automatically be excluded
#' @param k The number of clusters.
#' @param metric The distance metric used. Currently only 'euclidean' and 'manhattan' are supported.
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
#'
#' @importFrom cluster clara
clustering <- function(train, cluster_column = "cluster", exclude_columns = character(0), k = 4,
                           metric = "euclidean") {
    stopifnot(
        is.data.frame(train),
        is.character(exclude_columns),
        !any(!exclude_columns %in% colnames(train)),
        is.character(cluster_column),
        !cluster_column %in% colnames(train),
        is.character(metric),
        metric %in% c("euclidean", "manhattan"),
        is.numeric(k)
    )

    if(length(exclude_columns) > 0) train_cluster <- select_(train, .dots = paste0("-", exclude_columns))
    else train_cluster <- train

    # Make sure non-numeric columns are added to excluded columns
    non_numeric_columns <- !purrr::map_lgl(.x = train_cluster, .f = ~ is.numeric(.) || is.logical(.))
    if(any(non_numeric_columns)) {
        extra_excluded_columns <- colnames(train_cluster)[non_numeric_columns]
        train_cluster <- select_(train_cluster, .dots = paste0("-", extra_excluded_columns))
        exclude_columns <- unique(c(extra_excluded_columns, exclude_columns))
    }

    stopifnot(
        !any(!purrr::map_lgl(.x = train_cluster, .f = ~ is.numeric(.) || is.logical(.))),
        ncol(train_cluster) > 0
    )

    clustering <- cluster::clara(x = train_cluster, k = k, metric = metric, keep.data = F,
                                 correct.d = T,
                                 samples = ceiling(sqrt(nrow(train_cluster))), pamLike = T)


    train[cluster_column] <- as.character(clustering$clustering)
    clustering$medoids <- as_data_frame(clustering$medoids)

    predict_pipe <- pipe(.function = clustering_predict, metric = metric, centroids = clustering$medoids,
                         exclude_columns = exclude_columns, cluster_column = cluster_column)

    return(list(train = train, pipe = predict_pipe))
}

clustering_predict <- function(data, metric, centroids, cluster_column, exclude_columns) {
    stopifnot(
        is.data.frame(data),
        is.character(exclude_columns),
        !any(!exclude_columns %in% colnames(data)),
        is.character(cluster_column),
        !cluster_column %in% colnames(data),
        is.character(metric),
        metric %in% c("euclidean", "manhattan"),
        is.data.frame(centroids)
    )

    if(length(exclude_columns) > 0) data_cluster <- select_(data, .dots = paste0("-", exclude_columns))
    else data_cluster <- data

    stopifnot(ncol(centroids) == ncol(data_cluster))

    if(metric == "euclidean") dist_func <- function(center, points) {
        individual_dists <- purrr::map2_df(.x = center, .y = points, .f = function(x,y) (x-y) ^ 2)
        dists <- apply(individual_dists, 1, function(x) sqrt(sum(x, na.rm = T)))
        return(dists)
    } else if(metric == "manhattan") dist_func <- function(center, points) {
        individual_dists <- purrr::map2_df(.x = center, .y = points, .f = function(x,y) abs(x-y))
        dists <- apply(individual_dists, 1, function(x) sum(x, na.rm = T))
        return(dists)
    }

    clusters <- apply(centroids, 1, dist_func, points = data_cluster) %>%
        apply(1, which.min)

    stopifnot(length(clusters) == nrow(data_cluster))

    data[cluster_column] <- as.character(clusters)
    return(data)
}
