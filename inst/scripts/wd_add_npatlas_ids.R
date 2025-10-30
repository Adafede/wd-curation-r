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

# Filter valid and invalid Drugbank IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
npatlas_ids_ok <- npatlas_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

npatlas_ids_not_ok <- npatlas_ids |>
  tidytable::anti_join(npatlas_ids_ok)

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- npatlas |>
  tidytable::anti_join(
    npatlas_ids_ok,
    # by = c("id" = "structure_id_npatlas", "inchikey" = "inchikey")
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
    P7746 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P7746, S11797, s235)

deprecate_final <- npatlas_ids |>
  tidytable::inner_join(npatlas_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_npatlas = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_npatlas,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_npatlas) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P7746|"',
      structure_id_npatlas,
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
