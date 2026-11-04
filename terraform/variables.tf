variable "prefix" {
  type        = string
  default     = "dataplat"
  description = "Lowercase prefix for resource names."
}

variable "storage_suffix" {
  type        = string
  default     = "ref001"
  description = "Globally unique lowercase suffix for the storage account."
}

variable "location" {
  type        = string
  default     = "newzealandnorth"
  description = "Azure region."
}
