if (!requireNamespace("curl", quietly = TRUE)) {
  install.packages("curl")
}
if (!requireNamespace("stringi", quietly = TRUE)) {
  install.packages("stringi")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_hmdb <- "https://hmdb.ca/system/downloads/current/structures.zip"
path_output_qs <- "data/hmdb/hmdb_ids.csv"
path_output_ranks <- "data/hmdb/hmdb_ranks.csv"

sparql_hmdb <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_hmdb ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P2057 ?statement.
    ?statement ps:P2057 ?structure_id_hmdb.
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

temp_zip <- tempfile(fileext = ".zip")
curl::curl_download(
  url = path_hmdb,
  destfile = temp_zip,
  quiet = FALSE
)
temp_dir <- tempdir()
utils::unzip(
  zipfile = temp_zip,
  exdir = temp_dir
)
hmdb_structures <- file.path(
  temp_zip |>
    dirname(),
  path_hmdb |>
    basename() |>
    gsub(
      pattern = ".zip",
      replacement = ".sdf",
      fixed = TRUE
    )
)
sdf_data <- readLines(
  con = hmdb_structures,
  warn = FALSE
)

find_fixed_pattern_line_in_file <- function(file, pattern) {
  return(
    file |>
      stringi::stri_detect_fixed(pattern = pattern) |>
      which()
  )
}

return_next_line <- function(x, file) {
  file[x + 1]
}

patterns <- list(
  "id" = "> <DATABASE_ID>",
  "smiles" = "> <SMILES>",
  ## Not needed
  # "inchi" = "> <INCHI_IDENTIFIER>",
  "inchikey" = "> <INCHI_KEY>",
  "formula" = "> <FORMULA>",
  ## Because they do not have the same number of entries (weirdly...)
  # "mass" = "> <EXACT_MASS>",
  # "logp" = "> <JCHEM_LOGP>",
  "name" = "> <GENERIC_NAME>"
)

hmdb_df <- patterns |>
  lapply(FUN = find_fixed_pattern_line_in_file, file = sdf_data) |>
  lapply(
    FUN = return_next_line,
    file = sdf_data
  ) |>
  data.frame()

hmdb_prepared <- hmdb_df |>
  tidytable::mutate(
    tidytable::across(
      .cols = tidytable::everything(),
      .fns = tidytable::na_if,
      ""
    )
  ) |>
  tidytable::filter(!is.na(inchikey)) |>
  tidytable::select(
    id,
    inchikey
  ) |>
  tidytable::distinct()

file.remove(hmdb_structures)

hmdb_ids <- sparql_hmdb |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata()

# Filter valid and invalid HMDB IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
hmdb_ids_ok <- hmdb_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

hmdb_ids_not_ok <- hmdb_ids |>
  tidytable::anti_join(hmdb_ids_ok) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- hmdb_prepared |>
  tidytable::anti_join(
    hmdb_ids_ok,
    # by = c("id" = "structure_id_hmdb", "inchikey" = "inchikey")
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
    P2057 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P2057, S11797, s235)

deprecate_final <- hmdb_prepared |>
  tidytable::inner_join(hmdb_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_hmdb = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_hmdb,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_hmdb) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P2057|"',
      structure_id_hmdb,
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
