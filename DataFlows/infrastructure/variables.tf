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

variable "dataflow_bucket" {

    type = string
    description = "Name of dataflow_bucket"
    default = "ddl-dataflows"

}

variable "dataflow_dataset" {

    type = string
    description = "Name of dataflow_bucket"
    default = "df_dataset"

}

variable "sensor_data_table" {

    type = string
    description = "Name of dataflow_bucket"
    default = "sensor-data"

}