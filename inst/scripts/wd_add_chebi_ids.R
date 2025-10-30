if (!requireNamespace("stringi", quietly = TRUE)) {
  install.packages("stringi")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
if (!requireNamespace("WikidataQueryServiceR", quietly = TRUE)) {
  install.packages("WikidataQueryServiceR")
}

path_chebi <- "https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/structures.tsv.gz"
path_output_qs <- "data/chebi/chebi_ids.csv"
path_output_ranks <- "data/chebi/chebi_ranks.csv"

sparql_chebi <- "
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
SELECT ?structure ?structure_id_chebi ?inchi ?mapping_type ?statement ?statement_inchi WHERE {
    ?structure p:P683 ?statement.
    ?statement ps:P683 ?structure_id_chebi.
    ?structure wdt:P234 ?inchi.
    OPTIONAL {
      ?statement pq:P4390 ?mapping_type.
    }
    OPTIONAL {
      ?statement prov:wasDerivedFrom [
        pr:P234 ?statement_inchi
      ].
    }
}
"

sparql_inchis <- "SELECT * WHERE { ?structure wdt:P234 ?inchi. }"

# Load and filter ChEBI data
chebi <- path_chebi |>
  tidytable::fread() |>
  ## COMMENT: Remove those (see sucrose (CHEBI:17992) vs CHEBI:65313)
  tidytable::filter(
    !stringi::stri_detect_fixed(str = molfile, pattern = "M  STY"),
    !is.na(standard_inchi),
    standard_inchi != ""
  ) |>
  tidytable::distinct(id = compound_id, inchi = standard_inchi)

# Query Wikidata
chebi_ids <- WikidataQueryServiceR::query_wikidata(sparql_chebi)
inchis <- WikidataQueryServiceR::query_wikidata(sparql_inchis)

# Filter valid and invalid ChEBI IDs
## COMMENT: Done this way in case the item has 2 InChIs
chebi_ids_ok <- chebi_ids |>
  tidytable::group_by(structure) |>
  tidytable::filter(is.element(el = statement_inchi, set = inchi))

## COMMENT: Do not deprecate close matches, only exact or empty ones
chebi_ids_not_ok <- chebi_ids |>
  tidytable::anti_join(chebi_ids_ok) |>
  tidytable::mutate(
    mapping_type = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = mapping_type,
      fixed = TRUE
    )
  ) |>
  tidytable::filter(
    mapping_type == "Q39893449" |
      is.na(mapping_type)
  )

# Prepare additions
## COMMENT: Accept when one out of many IDs is mapped for the same InChI
add_final <- chebi |>
  tidytable::anti_join(
    chebi_ids_ok,
    # by = c("id" = "structure_id_chebi", "inchi" = "inchi")
    by = c("inchi" = "inchi")
  ) |>
  tidytable::inner_join(inchis, by = c("inchi" = "inchi")) |>
  tidytable::distinct() |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    qid = structure,
    P683 = paste0('"""', id, '"""'),
    qal4390 = "Q39893449",
    S11797 = "Q203250",
    s234 = paste0('"""', inchi, '"""')
  ) |>
  tidytable::select(qid, P683, qal4390, S11797, s234)

## COMMENT: Deprecation instead of deletion
deprecate_final <- chebi_ids |>
  tidytable::inner_join(chebi_ids_not_ok) |>
  tidytable::mutate(
    structure = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure,
      fixed = TRUE
    ),
    structure_id_chebi = gsub(
      pattern = "http://www.wikidata.org/entity/",
      replacement = "",
      x = structure_id_chebi,
      fixed = TRUE
    )
  ) |>
  tidytable::distinct(qid = structure, structure_id_chebi) |>
  tidytable::mutate(
    ranker = paste0(qid, '|P683|"', structure_id_chebi, '"|R-|P2241|Q25895909')
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
