if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_drugbank <- "https://go.drugbank.com/releases/latest/downloads/all-drugbank-vocabulary"
path_output_qs <- "data/drugbank/drugbank_ids.csv"
path_output_ranks <- "data/drugbank/drugbank_ranks.csv"

sparql_drugbank <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_drugbank ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P715 ?statement.
    ?statement ps:P715 ?structure_id_drugbank.
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

drugbank <- path_drugbank |>
  tidytable::fread(select = c("DrugBank ID", "Standard InChI Key")) |>
  tidytable::distinct(id = `DrugBank ID`, inchikey = `Standard InChI Key`) |>
  tidytable::filter(inchikey != "")

drugbank_ids <- sparql_drugbank |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata()

# Filter valid and invalid Drugbank IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
drugbank_ids_ok <- drugbank_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

drugbank_ids_not_ok <- drugbank_ids |>
  tidytable::anti_join(drugbank_ids_ok) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- drugbank |>
  tidytable::anti_join(
    drugbank_ids_ok,
    # by = c("id" = "structure_id_drugbank", "inchikey" = "inchikey")
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
    P715 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P715, S11797, s235)

deprecate_final <- drugbank_ids |>
  tidytable::inner_join(drugbank_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_drugbank = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_drugbank,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_drugbank) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P715|"',
      structure_id_drugbank,
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
