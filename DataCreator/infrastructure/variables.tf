variable "project" {
    type = string
    description = "Name of project"
    default = "dummydatalake"

}

variable "region" {

    type = string
    description = "Name of region"
    default = "us-central1"

}

variable "location" {

    type = string
    description = "Name of location"
    default = "US"

}

variable "zone" {

    type = string
    description = "Name of zone"
    default = "us-central1-c"

}

variable "data_creator_topic" {

    type = string
    description = "Name of data_creator_topic"
    default = "input_topic"

}