if (!requireNamespace("curl", quietly = TRUE)) {
  install.packages("curl")
}
if (!requireNamespace("DBI", quietly = TRUE)) {
  install.packages("DBI")
}
if (!requireNamespace("duckdb", quietly = TRUE)) {
  install.packages("duckdb")
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

path_pubchem <- "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-InChI-Key.gz"
path_output_qs <- "data/pubchem/pubchem_ids.csv"
path_output_ranks <- "data/pubchem/pubchem_ranks.csv"

sparql_pubchem <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_pubchem ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P662 ?statement.
    ?statement ps:P662 ?structure_id_pubchem.
    ?structure wdt:P235 ?inchikey.
    OPTIONAL {
      ?statement prov:wasDerivedFrom [
        pr:P235 ?statement_inchikey
      ].
    }
}
"
sparql_inchikeys <- "SELECT * WHERE { ?structure wdt:P235 ?inchikey. }"
pubchem_ids <- sparql_pubchem |>
  WikidataQueryServiceR::query_wikidata()
inchikeys <- sparql_inchikeys |>
  WikidataQueryServiceR::query_wikidata() |>
  as.data.frame()

# Create a connection
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(conn = con, statement = "INSTALL httpfs;")
DBI::dbExecute(conn = con, statement = "LOAD httpfs;")
DBI::dbWriteTable(
  conn = con,
  name = "inchikeys",
  value = inchikeys,
  overwrite = TRUE
)
local_file <- "CID-InChI-Key.gz"
if (!file.exists(local_file)) {
  curl::curl_download(
    url = path_pubchem,
    destfile = local_file,
    quiet = FALSE
  )
}
library(duckplyr)
pubchem <- local_file |>
  duckplyr::read_csv_duckdb() |>
  select(-column1) |>
  mutate(
    column2 = gsub(pattern = "^\\s+|\\s+$", replacement = "", x = column2)
  ) |>
  inner_join(inchikeys, by = c("column2" = "inchikey")) |>
  select(-structure) |>
  collect()

# Clean up
# file.remove(local_file)

# Filter valid and invalid PubChem IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
pubchem_ids_ok <- pubchem_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(
    is.element(el = statement_inchikey, set = inchikey)
  )

pubchem_ids_not_ok <- pubchem_ids |>
  tidytable::anti_join(pubchem_ids_ok) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- pubchem |>
  tidytable::anti_join(
    pubchem_ids_ok,
    # by = c("id" = "structure_id_pubchem", "inchikey" = "inchikey")
    by = c("column2" = "inchikey")
  ) |>
  tidytable::inner_join(inchikeys, by = c("column2" = "inchikey")) |>
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
    P662 = paste0("\"\"\"", column0, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", column2, "\"\"\"")
  ) |>
  tidytable::select(qid, P662, S11797, s235)

deprecate_final <- pubchem_ids |>
  tidytable::inner_join(pubchem_ids_not_ok) |>
  tidytable::tidytable() |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_pubchem = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_pubchem,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_pubchem) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P662|"',
      structure_id_pubchem,
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
