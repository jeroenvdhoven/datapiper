#' Removes highly correlated features whilst keeping as many as possible, using heuristics.
#'
#' @param train The train dataset, as a data.frame or data.table. Data.tables may be changed by reference.
#' @param exclude_columns Columns that should not be considered for removal.
#' @param threshold If the correlation is between two columns is larger than this, both will be considered for removal
#'
#' @return A list containing the transformed train dataset and a trained pipe.
#' @export
#' @importFrom igraph graph.data.frame V
pipe_remove_high_correlation_features <- function(train, exclude_columns = character(0), threshold = .8) {
    stopifnot(
        is.data.frame(train),
        !any(!exclude_columns %in% colnames(train)),
        is.numeric(threshold), threshold <= 1, threshold > 0
    )
    numeric_df <- train[, !purrr::map_lgl(train, is.character)]

    node_df <- high_correlation_features(numeric_df, exclude_columns, threshold) %>% igraph::graph.data.frame(directed = F)

    highly_correlated_features <- igraph::V(node_df)$name
    indep_cols <- greedy_max_independent_set(node_df)
    remove_cols <- highly_correlated_features %>% .[!. %in% indep_cols]
    keep_cols <- colnames(train) %>% .[!. %in% remove_cols]

    train <- select_cols(train, keep_cols)
    predict_pipe <- pipe(.function = preserve_columns_predict, preserved_columns = keep_cols)
    return(list("train" = train, "pipe" = predict_pipe))
}

#' Determines which columns are too highly correlated.
#'
#' @param data The data frame to use for determining which columns are highly correlated. Must be all numeric.
#' @param exclude_columns Columns that will not be used to determine too highly correlated features. Should include at least your response.
#' @param threshold If the absolute correlation between two variables is higher than this value, the pair will be registered as too highly correlated. Defaults to 0.8
#'
#' @return A dataframe with two columns, representing pairs of columns that are highly correlated
#' @export
high_correlation_features <- function(data, exclude_columns = character(0), threshold = .8){
    #ADD OPTION FOR STATISTICAL SIGNIFICANCE REMOVAL?
    stopifnot(!any(!exclude_columns %in% colnames(data)))

    if(length(exclude_columns) > 0) {
        data <- deselect_cols(data, exclude_columns, inplace = T)
    }
    stopifnot(!any(!purrr::map_lgl(data, ~ is.numeric(.) || is.logical(.))))

    correlation_matrix <- data %>% as.matrix %>% stats::cor(use = "complete.obs")
    correlation_matrix[upper.tri(correlation_matrix, diag = T)] <- 0

    highly_correlated_matrix <- as_data_frame(abs(correlation_matrix) >= threshold)

    correlated_names = purrr::map(.x = highly_correlated_matrix, .f = function(vec){
        rownames(correlation_matrix)[!is.na(vec) & vec]
    })

    correlated_names %<>% .[purrr::map_lgl(.x = ., .f = function(x) length(x) > 0)]
    if(length(correlated_names) == 0) return(data.frame(Node1 = "", Node2 = "", stringsAsFactors = F)[0,])

    df_list <- as.list(seq_len(length.out = length(correlated_names)))
    for(i in seq_along(correlated_names)){
        cur_var <- names(correlated_names)[i]
        cur_vector <- correlated_names[[i]]

        df_list[[i]] <- data.frame(Node1 = cur_var, Node2 = cur_vector, stringsAsFactors = F)
    }
    return(dplyr::bind_rows(df_list))
}


#' Plots highly correlated features as a graph
#'
#' @param high_cors Result of \code{\link[datapiper]{high_correlation_features}}. Can also provide your own graph generated using \code{\link[igraph]{graph.data.frame}}.
#' This is especially useful when combined with \code{\link[igraph]{decompose.graph}} to view subsections of the correlation graph.
#' @param ... arguments to be passed to plot.igraph. \code{vertex.size} and \code{vertex.label.cex} (for label size) are probably good parameters to pass.
#' @export
#' @importFrom igraph plot.igraph
plot_high_correlations <- function(high_cors, ...){
    if(is.data.frame(high_cors)) high_cors %<>% igraph::graph.data.frame(directed = F)
    if(missing(...)) igraph::plot.igraph(high_cors, vertex.size = 3, vertex.label.cex = 0.7)
    else igraph::plot.igraph(high_cors, ...)
}

#' Finds a maximum independent set using greedy search
#'
#' @param graph igraph to look for this set
#'
#' @return A set of maximum independent points
#' @importFrom igraph degree neighbors delete.vertices
greedy_max_independent_set <- function(graph){
    edges <- igraph::degree(graph)
    set <- character(0L)
    t_graph <- graph
    while(length(edges) > 0){
        pos <- names(edges)[edges == min(edges)][1]
        set %<>% c(pos)

        to_del <- c(pos, igraph::neighbors(t_graph, pos))
        t_graph %<>% igraph::delete.vertices(names(to_del) %>% .[. != ""] %>% c(pos))
        edges <- igraph::degree(t_graph)
    }
    return(set)
}
