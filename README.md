# Datapiper
### A collection of tools for building testable data pipelines and making them deployable.

## Introduction
The application of data science has been growing more and more in recent years. More companies seem to be using it in their day-to-day operations and interesting ways of tackling new problems emerge every day. Building a simple model is easy enough, but doing it well remains an expertise. Getting one into production is an entirely different problem altogether. There are a few problems that keep recurring in the process of preprocessing, training, and deploying, such as:

- Every preprocessing step applied to the train set needs to be repeated on the test and validation sets.
- Some preprocessing steps, like adding statistics, some forms of imputation, and one-hot encoding require a proper separation between train / test / validate sets. These steps use information in the train dataset and since test / validate sets are supposed to mimick new unseen data these steps can't use information from the test or validation data for their processing.
- Once a model is ready for deployment, the preprocessing steps need to be repeated on new and unseen data. Apart from that, the model often also needs to be able to integrate with other applications.

This is difficult enough. However, when we throw model selection, hyperparameter tuning, feature selection and different forms of preprocessing into the mix everything gets even more complicated.

At the core of these issues lie a couple of coding problems that have been solved before in regular software engineering. From my own experience, feature engineering and data preprocessing tends to be done in an on-the-fly way, not using functions / classes. This reduces the possibility of unit testing your code and introduces unnecessary code duplication. Not unit testing your code and instead duplicating your code greatly increase the chance of introducing bugs. A particular downside of bugs in data science is that the code might work just fine but not do what you want it to do (silently), or it works on the data you have at hand now but not on specific edge cases that haven't shown up yet.

Introducting statistics is difficult in its own right. The statistics have to be trained on the train dataset and merged with test and validate sets. However, say you calculate statistics for a categorical feature. What do you do when a new category shows up in your test or validation dataset? What do you do when a category occurs very little, increasing the chance of feature leakage? If so, what do you do when those categories show up in the test / validate datasets? These types of problems don't just occur with statistics, but also with other pre-processing steps, such as, one hot encoding and missing value imputation.

## Goal
This package tries to address some of the above mentioned problems. Our solution is based on one that already exists in the data science community: pipelines. Trainable sequences of data transformations that can be repeated on new, unseen data. However, we provide some additional functionality, such as:

- A large set of standardised and tested transformations.
- Wrappers for including common custom transformations so most if not all transformations can be included in the full pipeline.
- Easy testing of model, hyperparameter, and pipeline combinations.
- Making it easy to turn your pipelines (with or without model) into R packages and even Docker images.

All of this should help you be more efficient at testing different combinations of models and pipelines and bring machine learning models into production.

## Usage

### Intro

### Pipelines

We'll start by taking a standard dataset and splitting it into a train and test set.

```{r}
dataset <- datasets::airquality
training_indices <- sample.int(nrow(dataset), .7 * nrow(dataset))
train <- dataset[training_indices,]
test <- dataset[-training_indices,]
```

The next step is to build the pipeline. Say we are interested in predicting the temperature using this dataset. The column names aren't nicely standardised, so we can do that first. The date can also be added in using the month and day column. Next, the dataset has a few NA's in it, so we could apply some imputation. We can also generate some statistics for the month column.

```{r}
library(datapiper)
stat_functions <- list("mean" = mean)

basic_pipeline <- train_pipeline(
    segment(.segment = pipe_function, f = standard_column_names),
    segment(.segment = pipe_mutate, 
            date = "as.Date(paste0(1973, '-', month, '-', day))",
            distance_from_july = "as.numeric(difftime(as.Date(paste0(1973, '-', 07, '-', 16)), date, units = 'days'))",
            date = "as.numeric(date)"),
    segment(.segment = pipe_impute, columns = c("ozone", "solar_r"), type = "mean"),
    segment(.segment = pipe_create_stats, stat_cols = 'month', response = 'temp', 
            functions = stat_functions, too_few_observations_cutoff = 10),
    segment(.segment = pipe_select, "-month", "-day")
)

pipe_result <- basic_pipeline(train)
train_trans <- invoke(pipe_result$pipe, train)
test_trans <- invoke(pipe_result$pipe, test)

print(head(train_trans))
print(head(test_trans))
```

Now you have two datasets that followed the exact same transformations and a function that can reapply the same transformation to new datasets.

### Building a model

Once we have these datasets it's easy to train. We select different models with sets of parameters we want to test, define a function to optimise and include a set of pipelines. For this example we will only use the same pipeline as used above. The result of this test is a new dataframe, with the parameters, model and pipeline combined with the performance on the test and train sets.

```{r}
response <- "temp"

lm_model <- find_template_formula_and_data(response = response, training_function = lm)
rf_model <- find_template_formula_and_data(response = response, training_function = randomForest::randomForest, 
                                           ntree = c(10, 30, 50), nodesize = c(1,5, 10))

model_list <- list("lm" = lm_model, "rf" = rf_model)
pipe_list <- list("basic_pipe" = basic_pipeline)
rmse = function(x,y) mean(sqrt((x-y)^2))

model_results <- find_model(train = train, test = test, response = response, 
                            models = model_list, 
                            metrics = list("rmse" = rmse), 
                            prepend_data_checker = F, 
                            preprocess_pipes = pipe_list)

model_results <- model_results[order(model_results$test_rmse),]

print(find_expand_results(find_model_result = model_results))
```

With this result we can train our final model, combined with a pre-processing pipeline. This model can then be run on new data.

```{r}
model <- find_best_models(train = train, find_model_result = model_results, metric = "test_rmse", higher_is_better = F)

test_predictions <- invoke(model, test)
```

### Packaging

A good start to putting a model into production is to make it stand-alone. The pipeline helps us greatly with this, but we are still dependent on the environment we run in. The first step is to take our pipeline and put it in its own R package. This will allow us to easily transport the function without having to care about the dependencies (as much). The package will allow you to call your model using either a dataframe or JSON argument, so you can use it when deployed on a remote server and locally.

```{r}
package_name <- "temperature.predictor"
tar_file_name <- "temperature.tar.gz"
libraries <- c("randomForest", "datapiper")

build_model_package(
    trained_pipeline = model, 
    package_name = package_name, 
    libraries = libraries, 
    tar_file = tar_file_name, 
    may_overwrite_tar_file = F
)
install.packages(pkgs = "temperature.tar.gz", repos = NULL, type = "source")
```

Let's remove the variables we don't need anymore. The package should be stand-alone at this point
```{r}
current_vars <- ls()
rm(list = current_vars[!current_vars %in% c("train", "test", "package_name", "tar_file_name", "libraries")])
```

And to get the predictions:
```{r}
temperature.predictor::predict_model(train)
temperature.predictor::predict_model(test)
```

### Docker

The logical next step is to make the package less dependent on the machine it's running on. One way of doing this is by building a Docker image that serves our model. To this end we also use OpenCPU: a service that turns your R library into REST API's. This will allow us to create an image that can serve our model as a REST API when deployed. This has been tested only on MAC so far.

Note: building the image can take some time. Also, you need to have Docker installed.

```{r}
image_name = "temperature.image"
build_docker(model_library_file = tar_file_name, package_name = package_name, libraries = libraries, 
             docker_image_name = image_name, may_overwrite_docker_image = F)

test_docker(data = test, image_name = image_name, process_name = "docker.test", package_name = package_name, batch_size = 100)
```

## License

See LICENSE file

## Backlog

#### Pipelines and segments
- Automatically add in transformations that need to occur at the end of the pipeline, such as rescaling response variables.
- Allow custom models in impute / range_classifier
- Support sparklyr

#### Finding models
- Bayesian hyperparameter optimisation in find_model: investigate and optimise code.

#### Deployment
- Allow custom packages to be installed into the docker image more easily. With this we mean local packages or packages from private repositories
- Optimise building process

#### Other
- Visualise tool for sanity_check
- Expand toolkit for sanity_check

## Contact
