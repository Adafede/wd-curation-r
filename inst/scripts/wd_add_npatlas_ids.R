if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_npatlas <- "https://www.npatlas.org/static/downloads/NPAtlas_download.tsv"
path_output_qs <- "data/npatlas/npatlas_ids.csv"
path_output_ranks <- "data/npatlas/npatlas_ranks.csv"

sparql_npatlas <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_npatlas ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P7746 ?statement.
    ?statement ps:P7746 ?structure_id_npatlas.
    ?structure wdt:P235 ?inchikey.
    OPTIONAL {
      ?statement prov:wasDerivedFrom [
        pr:P235 ?statement_inchikey
      ].
    }
}
"
sparql_inchikeys <- "
SELECT * WHERE {
    ?structure wdt:P235 ?inchikey. hint:Prior hint:rangeSafe TRUE.
}
"

npatlas <- path_npatlas |>
  tidytable::fread(select = c("npaid", "compound_inchikey")) |>
  tidytable::distinct(id = npaid, inchikey = compound_inchikey)

npatlas_ids <- sparql_npatlas |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata()

npatlas_ids_ok <- npatlas_ids |>
  # Done this way in case the item has 2 InChIKeys
  tidytable::group_by(structure) |>
  tidytable::filter(statement_inchikey %in% inchikey)

npatlas_ids_not_ok <- npatlas_ids |>
  tidytable::anti_join(npatlas_ids_ok)

add_final <- npatlas |>
  tidytable::anti_join(
    npatlas_ids_ok,
    by = c("id" = "structure_id_npatlas", "inchikey" = "inchikey")
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
    P7746 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P7746, S11797, s235)

deprecate_final <- npatlas_ids |>
  tidytable::inner_join(npatlas_ids_not_ok) |>
  dplyr::mutate_all(
    ~ gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = .,
      fixed = TRUE
    )
  ) |>
  tidytable::rowwise() |>
  tidytable::mutate(
    qid = structure,
    statement = gsub(
      pattern = paste0("statement/", toupper(structure), "-"),
      replacement = "$",
      x = statement
    ),
    rank = "deprecated",
    reason = "Q25895909"
  ) |>
  tidytable::select(qid, statement, rank, reason) |>
  tidytable::distinct() |>
  tidytable::mutate(ranker = paste0(qid, statement, "|", rank, "|", reason)) |>
  tidytable::distinct(ranker)

# delete_final <- npatlas_ids |>
#   tidytable::inner_join(npatlas_ids_not_ok) |>
#   dplyr::mutate_all(~ gsub(
#     pattern = "http://www.wikidata.org/entity/",
#     replacement = "",
#     x = .,
#     fixed = TRUE
#   )) |>
#   tidytable::mutate(
#     qid = structure,
#     P7746 = paste0("\"\"\"", structure_id_npatlas, "\"\"\"")
#   ) |>
#   tidytable::distinct(qid, P7746)

source(file = "~/Git/hieroglyph/r/split_data_table.R")

if (nrow(add_final) != 0) {
  split_data_table(
    x = add_final,
    no_rows_per_frame = 5000,
    text = "npaids",
    path_to_store = "~/Documents/tmp/lotus/qs_addition/npatlas"
  )
}

if (nrow(deprecate_final) != 0) {
  split_data_table(
    x = deprecate_final,
    header = FALSE,
    no_rows_per_frame = 500,
    text = "npatlas",
    path_to_store = "~/Documents/tmp/lotus/ranker/npatlas"
  )
}

# if (nrow(delete_final) != 0) {
#   split_data_table(
#     x = delete_final,
#     no_rows_per_frame = 5000,
#     text = "npatlas",
#     path_to_store = "~/Documents/tmp/lotus/ranker/npatlas"
#   )
# }
