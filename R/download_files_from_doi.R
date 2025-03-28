#' Downloads DOI files from Zenodo
#'
#' This function downloads the deposits from ther DOI files from Zenodo.
#'
#' @param doi The DOI (Digital Object Identifier) of the deposit to be downloaded.
#' @param dest_dir The directory where the file(s) will be downloaded.
#' 
#' @return  invisibly `NULL`
#' 
#' @examples
#' \dontrun{
#' download_files_from_doi("10.1234/abcd")
#' }
#' @importFrom httr2 request req_perform resp_status resp_body_json
#' @importFrom utils download.file
#' 
#' @export
download_files_from_doi <- function(doi = NULL, dest_dir = ".") {
  if (is.null(doi)) stop("You must provide a DOI.")
  
  # Convert DOI to Zenodo-compatible ID
  base_url <- "https://zenodo.org/api/records/"
  record_id <- gsub("/", "-", doi)
  api_url <- paste0(base_url, record_id)

  # Perform request using httr2
  resp <- httr2::request(api_url) |>
    httr2::req_perform()

  # Check for errors manually (if desired)
  if (httr2::resp_status(resp) != 200) {
    stop("Failed to retrieve Zenodo record. Check the DOI.")
  }

  # Parse JSON from response
  record <- httr2::resp_body_json(resp)

  # Check if there are files
  if (is.null(record$files) || length(record$files) == 0) {
    message("No files found for this DOI.")
    return(invisible(NULL))
  }

  # Ensure destination directory exists
  if (!dir.exists(dest_dir)) {
    dir.create(dest_dir, recursive = TRUE)
  }

  # Download each file
  for (file in record$files) {
    dest_path <- file.path(dest_dir, basename(file$key))
    message("Downloading: ", file$key)
    utils::download.file(file$links$download, destfile = dest_path, mode = "wb")
  }

  message("Download complete.")
  return(invisible(NULL))
}

