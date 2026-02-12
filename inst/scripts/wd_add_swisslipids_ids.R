if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
source(
  file = "https://raw.githubusercontent.com/adafede/cascade/main/R/query_wikidata.R"
)

path_swisslipids <- "https://www.swisslipids.org/api/file.php?cas=download_files&file=lipids.tsv"
path_output_qs <- "data/swisslipids/swisslipids_ids.csv"
path_output_ranks <- "data/swisslipids/swisslipids_ranks.csv"

sparql_swisslipids <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_swisslipids ?inchikey ?statement ?statement_inchikey WHERE {
    ?structure p:P8691 ?statement.
    ?statement ps:P8691 ?structure_id_swisslipids.
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

swisslipids <- path_swisslipids |>
  tidytable::fread() |>
  tidytable::distinct(id = "Lipid ID", inchikey = "InChI key (pH7.3)") |>
  dplyr::mutate_all(
    ~ gsub(
      pattern = "InChIKey=",
      replacement = "",
      x = .,
      fixed = TRUE
    )
  ) |>
  tidytable::filter(inchikey != "") |>
  tidytable::filter(inchikey != "-")

swisslipids_ids <- sparql_swisslipids |>
  query_wikidata()
inchikeys <- sparql_inchikeys |>
  query_wikidata()

# Filter valid and invalid SwissLipids IDs
## COMMENT: Done this way in case the item has 2 InChIKeys
swisslipids_ids_ok <- swisslipids_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchikey, set = inchikey))

swisslipids_ids_not_ok <- swisslipids_ids |>
  tidytable::anti_join(swisslipids_ids_ok) |>
  tidytable::filter(!is.na(statement_inchikey)) |>
  tidytable::filter(statement_inchikey != "")

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChIKey
add_final <- swisslipids |>
  tidytable::anti_join(
    swisslipids_ids_ok,
    # by = c("id" = "structure_id_swisslipids", "inchikey" = "inchikey")
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
    P8691 = paste0("\"\"\"", id, "\"\"\""),
    S11797 = "Q21445422",
    s235 = paste0("\"\"\"", inchikey, "\"\"\"")
  ) |>
  tidytable::select(qid, P8691, S11797, s235)

deprecate_final <- swisslipids_ids |>
  tidytable::inner_join(swisslipids_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_swisslipids = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_swisslipids,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_swisslipids) |>
  tidytable::mutate(
    ranker = paste0(
      qid,
      '|P8691|"',
      structure_id_swisslipids,
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
