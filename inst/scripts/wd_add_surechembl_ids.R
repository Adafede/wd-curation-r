if (!requireNamespace("curl", quietly = TRUE)) {
  install.packages("curl")
}
if (!requireNamespace("duckplyr", quietly = TRUE)) {
  install.packages("duckplyr")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_surechembl <- "https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/latest/compounds.parquet"
path_output_qs <- "data/surechembl/surechembl_ids.csv"
path_output_ranks <- "data/surechembl/surechembl_ranks.csv"

sparql_surechembl <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_surechembl ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P2877 ?statement.
    ?statement ps:P2877 ?structure_id_surechembl.
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

surechembl_ids <- sparql_surechembl |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata() |>
  as.data.frame()

local_file <- "compounds.parquet"
if (!file.exists(local_file)) {
  curl::curl_download(
    url = path_surechembl,
    destfile = local_file,
    quiet = FALSE
  )
}

library(duckplyr)
surechembl_df <- local_file |>
  duckplyr::read_parquet_duckdb() |>
  select(id, inchikey = inchi_key) |>
  inner_join(inchikeys, by = c("inchikey" = "inchikey")) |>
  select(-structure) |>
  collect() |>
  tidytable::tidytable()

# Clean up
# file.remove(local_file)

# Filter valid and invalid SureChEMBL IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
surechembl_ids_ok <- surechembl_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(
    is.element(el = statement_inchikey, set = inchikey)
  )

surechembl_ids_not_ok <- surechembl_ids_ok |>
  tidytable::anti_join(surechembl_ids) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- surechembl_df |>
  tidytable::anti_join(
    surechembl_ids_ok,
    # by = c("id" = "structure_id_surechembl", "inchikey" = "inchikey")
    by = c("inchikey" = "inchikey")
  ) |>
  tidytable::inner_join(inchikeys, by = c("inchikey" = "inchikey")) |>
  tidytable::tidytable() |>
  tidytable::distinct() |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    qid = structure,
    P2877 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P2877, S11797, s235)

deprecate_final <- surechembl_ids |>
  tidytable::inner_join(surechembl_ids_not_ok) |>
  tidytable::tidytable() |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_surechembl = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_surechembl,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_surechembl) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P2877|"',
      structure_id_surechembl,
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
