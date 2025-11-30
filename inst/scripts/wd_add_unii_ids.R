if (!requireNamespace("curl", quietly = TRUE)) {
  install.packages("curl")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_unii <- "https://precision.fda.gov/uniisearch/archive/latest/UNII_Data.zip"
path_output_qs <- "data/unii/unii_ids.csv"
path_output_ranks <- "data/unii/unii_ranks.csv"

sparql_unii <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_unii ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P652 ?statement.
    ?statement ps:P652 ?structure_id_unii.
    ?structure wdt:P235 ?inchikey.
    OPTIONAL {
      ?statement prov:wasDerivedFrom [
        pr:P235 ?statement_inchikey
      ].
    }
}
"

sparql_inchikeys <- "
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT * WHERE { ?structure wdt:P235 ?inchikey. }
"

# Download the ZIP to a temp file
temp_zip <- tempfile(fileext = ".zip")
curl::curl_download(
  url = path_unii,
  destfile = temp_zip,
  quiet = FALSE
)
temp_dir <- tempdir()
utils::unzip(
  zipfile = temp_zip,
  exdir = temp_dir
)
files_in_zip <- unzip(temp_zip, list = TRUE)$Name
unii_file <- files_in_zip[grepl("^UNII_Records_.*\\.txt$", files_in_zip)]
unii <- tidytable::fread(
  cmd = sprintf("unzip -p %s %s", temp_zip, unii_file)
) |>
  tidytable::distinct(id = "UNII", inchikey = "INCHIKEY")

unii_ids <- sparql_unii |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata()

# Filter valid and invalid SwissLipids IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
unii_ids_ok <- unii_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

unii_ids_not_ok <- unii_ids |>
  tidytable::anti_join(unii_ids_ok) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- unii |>
  tidytable::anti_join(
    unii_ids_ok,
    # by = c("id" = "structure_id_unii", "inchikey" = "inchikey")
    by = c("inchikey" = "inchikey")
  ) |>
  tidytable::inner_join(inchikeys, by = c("inchikey" = "inchikey")) |>
  tidytable::distinct() |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    qid = structure,
    P652 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P652, S11797, s235)

add_final <- unii |>
  tidytable::anti_join(
    unii_ids_ok,
    by = c("id" = "structure_id_unii", "inchikey" = "inchikey")
  ) |>
  tidytable::inner_join(inchikeys, by = c("inchikey" = "inchikey")) |>
  tidytable::distinct() |>
  dplyr::mutate_all(
    ~ gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = .,
      fixed = TRUE
    )
  ) |>
  tidytable::mutate(
    qid = structure,
    P652 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P652, S11797, s235)

deprecate_final <- unii_ids |>
  tidytable::inner_join(unii_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_unii = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_unii,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_unii) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P652|"',
      structure_id_unii,
      '"|R-|P2241|Q25895909'
    )
  ) |>
  tidytable::distinct(ranker)

# Write outputs
write_output <- function(data, path) {
  if (nrow(data) > 0) {
    dir <- dirname(path)
    if (!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE)
    }
    tidytable::fwrite(
      data,
      path,
      quote = FALSE,
      col.names = path == path_output_qs
    )
  }
}

write_output(add_final, path_output_qs)
write_output(deprecate_final, path_output_ranks)
