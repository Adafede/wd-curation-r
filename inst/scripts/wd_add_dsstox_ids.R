if (!requireNamespace("readxl", quietly = TRUE)) {
  install.packages("readxl")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_dsstox <- "https://clowder.edap-cluster.com/files/6616d8d7e4b063812d70fc95/blob"
path_output_qs <- "data/dsstox/dsstox_ids.csv"
path_output_ranks <- "data/dsstox/dsstox_ranks.csv"

sparql_dsstox <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_dsstox ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P3117 ?statement.
    ?statement ps:P3117 ?structure_id_dsstox.
    ?structure wdt:P235 ?inchikey.
    OPTIONAL {
      ?statement prov:wasDerivedFrom [
        pr:P235 ?statement_inchikey
      ].
    }
}
"

sparql_inchikeys <- "SELECT * WHERE { ?structure wdt:P235 ?inchikey. }"

temp_zip <- tempfile(fileext = ".zip")
options(timeout = max(300, getOption("timeout")))
download.file(url = path_dsstox, destfile = temp_zip, mode = "wb")
temp_dir <- tempdir()
dsstox <- lapply(
  X = utils::unzip(zipfile = temp_zip, list = TRUE)$Name,
  FUN = function(x) {
    readxl::read_xlsx(
      utils::unzip(
        zipfile = temp_zip,
        files = x,
        exdir = temp_dir
      )
    ) |>
      tidytable::distinct(id = DTXSID, inchikey = INCHIKEY) |>
      tidytable::filter(!is.na(inchikey))
  }
) |>
  tidytable::bind_rows()
unlink(temp_zip)

dsstox_ids <- sparql_dsstox |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata()

# Filter valid and invalid DSSTOX IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
dsstox_ids_ok <- dsstox_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

dsstox_ids_not_ok <- dsstox_ids |>
  tidytable::anti_join(dsstox_ids_ok)

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- dsstox |>
  tidytable::anti_join(
    dsstox_ids_ok,
    # by = c("id" = "structure_id_dsstox", "inchikey" = "inchikey")
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
    P3117 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P3117, S11797, s235)

deprecate_final <- dsstox_ids |>
  tidytable::inner_join(dsstox_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_dsstox = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_dsstox,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_dsstox) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P3117|"',
      structure_id_dsstox,
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
