#' Downloads informations about the deposits of a Zenodo Community and saves them as json and parquet files
#'
#' This function downloads deposits from a Zenodo community and saves them as parquet files.
#' Only published deposits are downloaded.
#'
#' @param community The Zenodo community to download deposits from.
#' @param path The directory to save the parquet files to.
#' 
#' @return  The fully qualified file names of the files created:
#'    - `records.json`: the json file as received upon the first request
#'    - `hits.parquet`: the parquet file containing all the individual deposits
#'    - `metadata.parquet`: the parquet file containing the metadata for all the individual deposits
#'    - `related.parquet`: the parquet file containing the related identifiers for all the individual deposits including their relatinship
#' 
#' @importFrom httr2 request req_perform resp_status
#' @importFrom utils download.file
#' @importFrom DBI dbConnect dbDisconnect dbExecute
#' @importFrom duckdb duckdb
#'
#' @export
#'
#' @md
get_metadata_community_deposits <- function(
    community = "ipbes",
    path = paste0(community, "_deposits")
    ) {
  
  community_data <- httr2::request(paste0("https://zenodo.org/api/communities/", community)) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_perform()

  if (httr2::resp_status(community_data) == 404) {
    stop("Community '", community, "' does not exist.")
  }

  if (dir.exists(path)) {
    if (length(list.files(path)) > 0) {
      stop("Directory is not empty. Plese empty directory or delete it and try again!")
    }
  } else {
    dir.create(path)
  }

  download.file(
    url = httr2::resp_body_json(community_data)$links$records,
    destfile = file.path(path, "records.json")
  )


  # Convert to json

  message("Converting to parquet files ...")

  con <- DBI::dbConnect(duckdb::duckdb(), read_only = FALSE)

  on.exit(
    DBI::dbDisconnect(con, shutdown = TRUE)
  )

  DBI::dbExecute(conn = con, "INSTALL json")
  DBI::dbExecute(conn = con, "LOAD json")
  ##
  paste0("
  CREATE VIEW results AS
  SELECT
    UNNEST(hits, max_depth := 2)
  FROM read_json_auto('", file.path(path, "records.json"), "');
  ") |>
    DBI::dbExecute(conn = con)
  #
  paste0("
  CREATE VIEW hits AS
  SELECT
    hit_struct.*
  FROM
    results,
    UNNEST(hits) AS t(hit_struct);
  ") |>
    DBI::dbExecute(conn = con)
  #
  paste0("
  CREATE VIEW metadata AS
  SELECT
    doi_url,
    UNNEST(metadata) FROM hits;
  ") |>
    DBI::dbExecute(conn = con)
  #
  paste0("
  CREATE VIEW related AS
  SELECT
     doi_url,
     ri.*
   FROM
     metadata,
     UNNEST(related_identifiers) AS t(ri);
  ") |>
    DBI::dbExecute(conn = con)
  #
  # Save to parquet files
  #
  paste0("
    COPY hits
    TO '", file.path(path, "hits.parquet"), "'
    (FORMAT PARQUET, COMPRESSION SNAPPY)
    ") |>
    DBI::dbExecute(conn = con)
  #
  paste0("
    COPY metadata
    TO '", file.path(path, "metadata.parquet"), "'
    (FORMAT PARQUET, COMPRESSION SNAPPY)
    ") |>
    DBI::dbExecute(conn = con)
  #
  paste0("
    COPY related
    TO '", file.path(path, "related.parquet"), "'
    (FORMAT PARQUET, COMPRESSION SNAPPY)
    ") |>
    DBI::dbExecute(conn = con)
  #
  return(list.files(path, full.names = TRUE))
}

