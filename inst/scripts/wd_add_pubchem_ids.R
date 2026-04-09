if (!requireNamespace("curl", quietly = TRUE)) {
  install.packages("curl")
}
if (!requireNamespace("tidytable", quietly = TRUE)) {
  install.packages("tidytable")
}
source(
  file = "https://raw.githubusercontent.com/adafede/cascade/main/R/query_wikidata.R"
)

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

sparql_inchikeys <- "
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT * WHERE { ?structure wdt:P235 ?inchikey. }
"

# Query Wikidata
pubchem_ids <- query_wikidata(
  sparql_pubchem,
  endpoint = "https://qlever.dev/api/wikidata"
)
inchikeys <- query_wikidata(
  sparql_inchikeys,
  endpoint = "https://qlever.dev/api/wikidata"
)

# Fetch PubChem data via QLever
fetch_pubchem_chunk <- function(
  keys,
  endpoint = "https://qlever.dev/api/pubchem"
) {
  keys <- unique(keys)
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (length(keys) == 0L) {
    return(data.table::data.table(cid = character(), inchikey = character()))
  }

  sparql <- paste0(
    'PREFIX vocab: <http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT ?cid ?inchikey WHERE {
  VALUES ?inchikey {
',
    paste0('    "', keys, '"', collapse = "\n"),
    '
  }
  ?cpd dcterms:identifier ?cid ;
       vocab:inchikey ?inchikey .
}'
  )

  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)

  h <- curl::new_handle()
  curl::handle_setopt(
    h,
    post = 1L,
    postfields = paste0("query=", curl::curl_escape(sparql)),
    timeout = 600L,
    followlocation = 1L
  )
  curl::handle_setheaders(
    h,
    "Accept" = "text/csv",
    "Accept-Encoding" = "gzip, deflate",
    "Content-Type" = "application/x-www-form-urlencoded"
  )

  curl::curl_download(endpoint, destfile = tmp, handle = h, quiet = TRUE)

  # Fail fast when endpoint returns JSON/HTML error text instead of CSV.
  preview <- readLines(tmp, n = 5L, warn = FALSE, encoding = "UTF-8")
  first_line <- if (length(preview)) trimws(preview[1]) else ""
  if (
    startsWith(first_line, "{") ||
      startsWith(first_line, "<") ||
      grepl('"status"\\s*:\\s*"ERROR"', paste(preview, collapse = "\n"))
  ) {
    stop(
      paste0(
        "PubChem endpoint returned a non-CSV payload. First line: ",
        substr(first_line, 1L, 240L)
      ),
      call. = FALSE
    )
  }

  dt <- data.table::fread(
    tmp,
    sep = ",",
    header = TRUE,
    encoding = "UTF-8",
    quote = "",
    showProgress = FALSE,
    data.table = TRUE
  )

  if (ncol(dt) == 0L) {
    return(data.table::data.table(cid = character(), inchikey = character()))
  }

  normalized_names <- tolower(gsub('^\\?+', "", names(dt)))
  data.table::setnames(dt, names(dt), normalized_names)
  required_cols <- c("cid", "inchikey")
  if (!all(required_cols %in% names(dt))) {
    stop(
      paste0(
        "PubChem CSV schema mismatch. Expected columns: cid, inchikey. Got: ",
        paste(names(dt), collapse = ", ")
      ),
      call. = FALSE
    )
  }

  dt <- dt[, .(
    cid = as.character(cid),
    inchikey = as.character(inchikey)
  )]
  dt[!is.na(inchikey) & nzchar(inchikey)]
}

fetch_pubchem_chunk_resilient <- function(
  keys,
  endpoint = "https://qlever.dev/api/pubchem",
  min_chunk_size = 20000L
) {
  tryCatch(
    fetch_pubchem_chunk(keys = keys, endpoint = endpoint),
    error = function(e) {
      n <- length(keys)
      if (n <= min_chunk_size || n <= 1L) {
        stop(e)
      }
      split_at <- as.integer(ceiling(n / 2))
      message(
        sprintf(
          "Chunk of %d keys failed (%s). Retrying as %d + %d.",
          n,
          conditionMessage(e),
          split_at,
          n - split_at
        )
      )
      data.table::rbindlist(
        list(
          fetch_pubchem_chunk_resilient(
            keys[seq_len(split_at)],
            endpoint = endpoint,
            min_chunk_size = min_chunk_size
          ),
          fetch_pubchem_chunk_resilient(
            keys[seq.int(split_at + 1L, n)],
            endpoint = endpoint,
            min_chunk_size = min_chunk_size
          )
        ),
        use.names = TRUE
      )
    }
  )
}

chunk_size <- 50000L
chunks <- split(
  inchikeys$inchikey,
  ceiling(seq_along(inchikeys$inchikey) / chunk_size)
)
pubchem_2 <- data.table::rbindlist(
  lapply(seq_along(chunks), function(i) {
    message(sprintf(
      "Chunk %d / %d (%d keys)...",
      i,
      length(chunks),
      length(chunks[[i]])
    ))
    fetch_pubchem_chunk_resilient(chunks[[i]])
  }),
  use.names = TRUE
)

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
add_final <- pubchem_2 |>
  tidytable::anti_join(
    pubchem_ids_ok,
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
    P662 = paste0('"""', cid, '"""'),
    S11797 = "Q21445422",
    s235 = paste0('"""', inchikey, '"""')
  ) |>
  tidytable::select(qid, P662, S11797, s235)

deprecate_final <- pubchem_ids |>
  tidytable::inner_join(pubchem_ids_not_ok) |>
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
