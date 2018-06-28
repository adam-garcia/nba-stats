#   ____________________________________________________________________________
#   0-main.R                                                                ####
#   purpose: load script dependencies, organize program flow
#   dependencies: none

# Package Loading
library(tidyverse)
library(curl)
library(jsonlite)
library(purrr)
library(broom)
library(glue)
library(lcmm)
library(feather)

# Options
theme_set(theme_light())
options(scipen=999)

# Helper functions
preview <- function(tbl){
  tbl %>% 
    head(10) %>% 
    knitr::kable() %>% 
    kableExtra::kable_styling() %>%
    kableExtra::scroll_box(width = "100%")
}