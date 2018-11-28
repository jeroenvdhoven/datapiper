#' Find fitting models and test them using given metrics on the test dataset
#'
#' @param train The training dataset
#' @param test The testing dataset
#' @param response The response column as a string
#' @param preprocess_pipes List of preprocessing pipelines generated using \code{\link{pipeline}}.
#' @param models A list of models. Each model should be a list, containing at least a training function \code{.train} and a \code{.predict} function, plus named
#' vectors of parameters to explore.
#'
#' The \code{.train} function has to take a \code{data} argument that stores the training data and a \code{...} argument for the parameters.
#' The \code{.predict} function needs to take two arguments, where the first is the model and the second the new dataset.
#'
#' If a parameter only takes a single value, you can use a vector to store options. Otherwise use a list.
#'
#' You can use \code{\link{model_trainer}} as a wrapper for this list. It will also test your inputs.
#' @param metrics A list of metrics (functions) that need to be calculated on the train and test response and predictions. Must be named.
#'
#' @param target_metric The name of the metric to optimise. Optimisation will be done on the testset performance of this metric.
#' @param higher_is_better A flag indicating if a high value of \code{target_metric} indicates a good result.
#' @param N_init Number of iterations to initialise the bayesian optimisation with.
#' @param N_experiment Number of experimentations done with the bayesian optimisation.
#' \itemize{
#' \item A numeric vector with as many entries as \code{x}.
#' \item A numeric matrix with as many columns as entries in \code{x}.
#' }
#' @param sigma_noise An estimate of the inherent noise in sampling from
#'
#' @param prepend_data_checker Flag indicating if \code{\link{pipeline_check}} should be prepended before all pipelines.
#' @param on_missing_column See \code{\link{pipeline_check}} for details.
#' @param on_extra_column See \code{\link{pipeline_check}} for details.
#' @param on_type_error See \code{\link{pipeline_check}} for details.
#'
#' @param seed Random seed to set each time before a model is trained. Set this to 0 to ignore setting seeds.
#' @param verbose Should intermediate updates be printed.
#' @param save_model Flag indicating if the generated models should be saved. Defaults to False.
#'
#' @return A dataframe containing the training function, a list of parameters used to train the function, and one column for each metric / dataset combination.
#' @export
#' @importFrom purrr map_dbl map_lgl pmap_df
find_model_through_bayes <- function(
    train, test, response,
    preprocess_pipes = list(function(train, test) return(list(train = train, test = train, .predict = function(data) return(data)))),
    models, metrics,
    target_metric, higher_is_better,
    N_init = 20, N_experiment = 40,
    sigma_noise = 1e-8,
    prepend_data_checker = T,
    on_missing_column = c("error", "add")[1],
    on_extra_column = c("remove", "error")[1],
    on_type_error = c("ignore", "error")[1],
    seed = 1,
    verbose = T,
    save_model = F
) {
    # Check if incomming arguments are somewhat correct
    stopifnot(
        !missing(train), is.data.frame(train),
        !missing(test) , is.data.frame(test),
        is.character(response), length(response) == 1,
        is.logical(prepend_data_checker),
        is.logical(save_model),
        is.list(models),
        is.list(metrics), !any(!purrr::map_lgl(metrics, is.function)), !is.null(names(metrics)),
        is.list(preprocess_pipes), !any(!purrr::map_lgl(preprocess_pipes, is.function)),

        is.numeric(N_init), N_init > 0,
        is.numeric(N_experiment), N_experiment > 0,
        is.numeric(sigma_noise), sigma_noise >= 0,
        is.character(target_metric), target_metric %in% names(metrics)
    )

    # Check more detailed elements of input parameters
    models_have_valid_elements <- purrr::map_lgl(models, function(m) {
        if(any(!c(".train", ".predict") %in% names(m))) return(F)

        are_functions <- purrr::map_lgl(m[c(".train", ".predict")], is.function)
        return(!any(!are_functions))
    })

    if(is.null(names(models))) model_names <- seq_along(models)
    else model_names <- names(models)

    if(is.null(names(preprocess_pipes))) pipe_names <- seq_along(preprocess_pipes)
    else pipe_names <- names(preprocess_pipes)

    if(any(!models_have_valid_elements)) stop("Error: all models must contain .train and .predict elements that are functions")
    # if(seed != 0) sigma_noise <- 1e-24 # Since seeds are set at every model run, the standard deviation on a parameter set is by definition 0

    # Do setup for experimentation
    metric_names <- names(metrics)
    target_metric <- paste0("test_", target_metric)
    res <- data_frame(.train = list(), .predict = list(), .id = "", params = list(), .preprocess_pipe = list())[0,]
    for(metric_name in metric_names) {
        res[paste0("train_", metric_name)] <- numeric(0)
        res[paste0("test_", metric_name)] <- numeric(0)
    }
    if(save_model) res[".model"] <- list()

    # Try each preprocessing pipe
    for(preprocess_index in seq_along(preprocess_pipes)){
        if(verbose) cat(paste("Computing preprocess pipeline", preprocess_index, "/", length(preprocess_pipes), "\n"))

        preprocess_pipe <- preprocess_pipes[[preprocess_index]]
        if(prepend_data_checker){
            preprocess_pipe <- train_pipeline(
                segment(.segment = pipeline_check, response = response,
                        on_missing_column = on_missing_column, on_extra_column = on_extra_column, on_type_error = on_type_error),
                segment(.segment = preprocess_pipe))
        }
        piped <- preprocess_pipe(train)
        piped_train <- piped$train
        trained_pipeline <- piped$pipe
        piped_test <- invoke(trained_pipeline, test)

        # Make sure response is in the final training / testing dataset
        stopifnot(
            response %in% colnames(piped_train),
            response %in% colnames(piped_test)
        )

        # Try each model
        for(model_index in seq_along(models)) {
            if(verbose) cat(paste("\rComputing model", model_index, "/", length(models), "\n"))
            model <- models[[model_index]]

            current_results <- run_experiments(train = piped_train, test = piped_test, model = model, model_name = model_names[model_index],
                                   metrics = metrics, metric_names = metric_names, target_metric = target_metric, sigma_noise = sigma_noise,
                                   response = response, verbose = verbose, seed = seed, higher_is_better = higher_is_better,
                                   N_init = N_init, N_experiment = N_experiment)
            current_results[, ".id"] = paste0(pipe_names[preprocess_index], "_", model_names[model_index])
        }
        current_results[, ".preprocess_pipe"] = list(trained_pipeline)
        res <- rbind(res, current_results)
    }

    return(res)
}

run_experiments <- function(train, test, model, model_name, response,
                            metrics, metric_names, target_metric,
                            N_init, N_experiment, sigma_noise,
                            higher_is_better = higher_is_better,
                            verbose = T, seed = 1) {
    f_train <- model[[".train"]]
    f_predict <- model[[".predict"]]

    parameter_grid <- compute_parameter_grid_bayes(model = model, N_init = N_init, N_experiment = N_experiment)

    parameter_grid_size <- nrow(parameter_grid)
    if(N_init > parameter_grid_size) N_init <- parameter_grid_size
    if(N_experiment > parameter_grid_size - N_init) N_experiment <- parameter_grid_size - N_init

    performance <- numeric(N_init + N_experiment)
    test_indices <- sample.int(n = parameter_grid_size, size = N_init)

    res <- data_frame()
    for(i in seq_len(N_init)) {
        if(verbose) cat(paste("\rComputing iteration", i, "/", N_init + N_experiment, "out of a maximum of", parameter_grid_size, "iterations"))

        model_results <- test_model_configuration(train = train, test = test,
                                                  f_train = f_train, f_predict = f_predict,
                                                  parameters = parameter_grid[test_indices[i], ],
                                                  metric_names = metric_names, metrics = metrics, response = response,
                                                  seed = seed)
        # Test model and record performance
        performance[i] <- unlist(model_results[target_metric])
        if(nrow(res) == 0) res <- model_results
        else res[i, ] <- model_results
    }

    tested_indices <- numeric(length = N_experiment + N_init)
    tested_indices[seq_len(N_init)] <- test_indices
    parameter_grid_matrix <- as.matrix(parameter_grid)
    for(i in seq_len(N_experiment)) {
        if(verbose) cat(paste("\rComputing iteration", i + N_init, "/", N_init + N_experiment, "out of a maximum of", parameter_grid_size, "iterations"))

        # Select the next point to test.
        occupied_points <- seq_len(N_init + i - 1)
        tested_configurations <- parameter_grid_matrix[tested_indices[occupied_points], , drop = F]
        distributions <- distribution_at_x(x = parameter_grid_matrix, previous_X = tested_configurations,
                                           y = performance[occupied_points], sigma_noise = sigma_noise)

        y_max <- ifelse(higher_is_better, max(performance[occupied_points]), min(performance[occupied_points]))

        EI <- expected_improvement(mu = distributions$mu, sigma = distributions$sigma, y_max = y_max)
        EI_res[[length(EI_res) + 1]] <<- EI

        if(higher_is_better) search_function <- which.max
        else search_function <- which.min

        next_index <- -1
        while(next_index < 0) {
            possible_index <- search_function(EI)
            if(possible_index %in% tested_indices && !is.infinite(EI[next_index])) {
                EI[possible_index] <- ifelse(higher_is_better, -Inf, Inf)
            } else next_index <- possible_index
        }

        # if(next_index %in% test_indices) break
        tested_indices[i + N_init] <- next_index
        parameters_to_test <- parameter_grid[next_index, ]
        # Test model and record performance
        model_results <- test_model_configuration(train = train, test = test,
                                                  f_train = f_train, f_predict = f_predict, parameters = parameters_to_test,
                                                  metric_names = metric_names, metrics = metrics, response = response,
                                                  seed = seed)
        # Test model and record performance
        performance[i + N_init] <- unlist(model_results[target_metric])
        res[N_init + i, ] <- model_results
    }
    cat("\n")
    return(res)
}

compute_parameter_grid_bayes <- function(model, N_init, N_experiment) {
    parameter_grid <- expand.grid(stringsAsFactors = F, model[!names(model) %in% c(".train", ".predict")])
    parameter_grid_size <- nrow(parameter_grid)

    if(parameter_grid_size < 1) parameter_grid <- data_frame(1)[,0]

    parameter_grid <- as_data_frame(parameter_grid)
    return(parameter_grid)
}

test_model_configuration <- function(train, test, f_train, f_predict, metrics, response = response,
                                     parameters, metric_names, seed,
                                     save_model = F) {
    if(seed != 0) set.seed(seed)

    args <- list(data = train)
    args <- c(args, unlist(parameters))

    requested_arguments <- formalArgs(f_train)
    if(any(!names(args) %in% requested_arguments) && !"..." %in% requested_arguments) {
        faulty_args <- names(args)[!names(args) %in% requested_arguments]
        text_args <- paste0(collapse = ", ", faulty_args)
        stop(paste0("Warning: arguments `", text_args, "` were not arguments of the provided .train function"))
    }

    model <- do.call(what = f_train, args = args)

    # Do train and test predictions and calculate metrics
    train_preds <- f_predict(model, train)
    train_metrics_calculated <- purrr::map_dbl(.x = metrics, function(f) f(unlist(train[response]), train_preds))
    test_preds <- f_predict(model, test)
    test_metrics_calculated <- purrr::map_dbl(.x = metrics, function(f) f(unlist(test[response]), test_preds))

    tmp <- list(".train" = list(f_train), ".predict" = list(f_predict),
                "params" = list(parameters))
    tmp[paste0("train_", metric_names)] <- train_metrics_calculated
    tmp[paste0("test_", metric_names)] <- test_metrics_calculated
    if(save_model) tmp$.model <- list(model)

    tmp <- as_data_frame(tmp)
    return(tmp)
}

kernel_matrix <- function(X, kernel) {
    stopifnot(is.matrix(X), nrow(X) > 0)
    size_of_x <- nrow(X)

    res <- matrix(nrow = size_of_x, ncol = size_of_x)

    for(i in seq_len(size_of_x - 1)) {
        remaining_indices <- seq_len(size_of_x - i + 1) + i - 1
        res[i, remaining_indices] <- res[remaining_indices, i] <- kernel(X[i, ], X[remaining_indices, ])
    }

    res[size_of_x, size_of_x] <- kernel(X[size_of_x, ], X[size_of_x, ])
    return(res)
}

kernel_matrix_2_vec <- function(x_1, x_2, kernel, old = T) {
    size_of_x_1 <- nrow(x_1)
    size_of_x_2 <- nrow(x_2)

    res <- matrix(nrow = size_of_x_1, ncol = size_of_x_2)
    for(i in seq_len(size_of_x_2)) {
        res[, i] <- kernel(x_2[i, ], x_1)
    }
    return(res)
}

kernel_matrix_pairwise <- function(x, kernel) {
    size_of_x <- nrow(x)

    res <- numeric(size_of_x)
    for(i in seq_len(size_of_x)) {
        res[i] <- kernel(x[i, ], x[i, ])
    }
    return(res)
}

distribution_at_x <- function(x, previous_X, y, sigma_noise) {
    stopifnot(
        is.matrix(x),
        is.matrix(previous_X),
        is.numeric(y),
        length(y) == nrow(previous_X)
    )
    scales <- apply(x, 2, function(x) max(x) - min(x))
    scales[scales < 1e-6] <- 1
    # scales <- 1
    kernel = function(x,y) mattern_52_kernel(x,y, scales = scales)

    km <- kernel_matrix(X = previous_X, kernel = kernel) + diag(nrow(previous_X)) * sigma_noise
    km_inv <- solve(km)

    k_target <- kernel_matrix_2_vec(x, previous_X, kernel = kernel)
    k_target_km_inv <- k_target %*% km_inv

    mu <- as.numeric(k_target_km_inv %*% y)
    sigma <- numeric(length = nrow(x))

    for(i in seq_along(sigma)) sigma[i] <- k_target_km_inv[i, ] %*% k_target[i, ]
    self_kernel <- kernel_matrix_pairwise(x, kernel = kernel)

    sigma <- sigma * self_kernel
    return(data_frame("mu" = mu, "sigma" = sigma))
}

expected_improvement <- function(mu, sigma, y_max, tolerance = 1e-10) {
    stopifnot(
        is.numeric(mu),
        is.numeric(sigma),
        is.numeric(y_max),
        length(mu) == length(sigma),
        length(y_max) == 1
    )

    sigma_is_0 <- sigma < tolerance

    mu_diff <- mu - y_max
    res <- numeric(length = length(mu))
    res[sigma_is_0] <- ifelse(mu_diff[sigma_is_0] > 0, mu_diff[sigma_is_0], 0)

    Z <- mu_diff[!sigma_is_0] / sigma[!sigma_is_0]
    normal_pdf <- dnorm(x = Z)
    normal_cummulative <- pnorm(q = Z)

    res[!sigma_is_0] <- mu_diff[!sigma_is_0] * normal_cummulative + sigma[!sigma_is_0] * normal_pdf
    return(res)
}

gaussian_kernel <- function(x, y, sigma = 1) {
    stopifnot(
        is.numeric(x),
        is.numeric(y),
        (is.vector(y) && length(x) == length(y)) ||
            (is.matrix(y) && length(x) == ncol(y))
    )

    if(is.matrix(y)) {
        diffs <- sweep(x = y, MARGIN = 2, STATS = x) ^ 2
        distance <- apply(diffs, 1, sum)
    } else {
        distance <- sum((x-y)^2)
    }
    return(exp(-distance / 2 * sigma^2))
}


mattern_52_kernel <- function(x, y, scales = 1) {
    stopifnot(
        is.numeric(x),
        is.numeric(y),
        (is.vector(y) && length(x) == length(y)) ||
            (is.matrix(y) && length(x) == ncol(y)),
        is.numeric(scales),
        length(scales) == length(x) || length(scales) == 1
    )
    if(length(scales) == 1) scales <- rep(scales, length(x))
    if(any(scales == 0)) scales[scales == 0] <- 1

    if(is.matrix(y)) {
        diffs <- sweep(x = y, MARGIN = 2, STATS = x) ^ 2
        diffs <- sweep(x = diffs, MARGIN = 2, STATS = scales ^ 2, FUN = "/")
        distance <- apply(diffs, 1, sum)
    } else {
        diffs <- ((x-y) / scales) ^ 2
        distance <- sum((diffs)^2)
    }
    sqrt_five <- sqrt(5 * distance)
    res <- (1 + sqrt_five + 5 / 3 * distance ^ 2) * exp(-sqrt_five)
    return(res)
}
