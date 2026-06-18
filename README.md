# Africa Corpus Builder

A toolkit and small Python library for **retrieving parallel and monolingual
text corpora for African languages**. Pick any African language and pair it
with English, with another African language, or with one of several other world
languages — or pull a monolingual corpus for a single language. Output is
clean, sentence-aligned CSV, ready for machine-translation training and NLP
research.

The corpora are hosted on HuggingFace at
[`michsethowusu/africa-corpus`](https://huggingface.co/datasets/michsethowusu/africa-corpus).
The library downloads only the files you actually use and caches them locally,
so the repository itself stays lightweight. New languages pushed to the dataset
are picked up automatically — no code change or update needed.

---

## What you can build

| Corpus type | Example | Notes |
|---|---|---|
| African ↔ English | Swahili ↔ English | English ships cached; the default target |
| African ↔ African | Twi ↔ Yoruba, Hausa ↔ Amharic | align two local languages directly |
| African ↔ other language | Twi ↔ French, Zulu ↔ Arabic | French, Arabic, Chinese, Portuguese are cached |
| Monolingual | all Swahili sentences | any single language |

Every language's text is aligned on a shared verse key, so **any** two
languages can be turned into a parallel corpus by a simple join. That is what
makes African ↔ African and African ↔ other-language pairs possible.

---

## Quick start

```bash
git clone https://github.com/michsethowusu/africa-corpus-builder.git
cd africa-corpus-builder
```

Requires Python 3.10+ and `huggingface_hub` (used to download the data the
first time you reference a language):

```bash
pip install huggingface_hub
```

Downloaded files are cached, so each language is only fetched once. The dataset
is public — no HuggingFace login is needed to read it.

### List what's available

```bash
python africa_corpus.py --list
```

### Build a corpus for one language (the common case)

```bash
# Swahili ↔ English (English is the default target)
python africa_corpus.py --source swc

# Twi ↔ Yoruba (two African languages)
python africa_corpus.py --source twi --target yor

# Hausa ↔ French
python africa_corpus.py --source hau --target fr

# Monolingual Amharic
python africa_corpus.py --source amh --monolingual
```

Each writes a CSV named after the languages (e.g. `swc_en_parallel.csv`,
`amh_monolingual.csv`). Use `--out PATH` to choose the filename.

### Limit the number of samples

```bash
# first 5,000 Swahili–English pairs (in scripture order, deterministic)
python africa_corpus.py --source swc --limit 5000

# a random 5,000-pair sample (reproducible via --seed)
python africa_corpus.py --source swc --limit 5000 --sample --seed 42
```

### Build for many languages at once

`--source` accepts a comma-separated list or the keyword `all`. With more than
one source, one file per language is written into `--out-dir` (default
`corpora/`).

```bash
# every African language paired with English, 10k samples each
python africa_corpus.py --source all --limit 10000 --out-dir corpora/

# a selected set, paired with French
python africa_corpus.py --source twi,swc,yor,hau --target fr --out-dir corpora/

# monolingual corpora for every African language
python africa_corpus.py --source all --monolingual --out-dir corpora/
```

### Use it as a library

```python
import africa_corpus as ac

ac.list_languages()                               # (african, reference) language lists
rows  = ac.parallel("swc", "en", limit=1000)      # [(verse_key, swc, en), ...]
rows  = ac.parallel("twi", "yor")                 # twi ↔ Yoruba
sents = ac.monolingual("hau", limit=500, sample=True)

ac.write_parallel_csv("swc", "fr", "swahili_french.csv", limit=2000)
ac.write_monolingual_csv("amh", "amharic.csv")

# one file per language
ac.build_batch(ac.all_african_codes(), target="en",
               limit=10000, out_dir="corpora/")
```

Languages are referenced by code (`swc`, `yor`, `fr`) or by name
(`"Swahili"`, `"French"`).

---

## Available languages

**African languages** — 792+ languages across the continent are available. Run
`python africa_corpus.py --list` to see all codes and names.

**Reference languages** that can be used as the non-African side of a parallel corpus:

| Code | Language | Version |
|---|---|---|
| `en` | English | CEB (v37) |
| `fr` | French | LSG (v93) |
| `ar` | Arabic | AVD (v13) |
| `zh` | Chinese | CUNPSS (v48) |
| `pt` | Portuguese | ARA (v1608) |

### Adding more reference languages

The reference set is fully self-describing — no index and no code changes. Each
cache is stored as `reference_caches/{Name}_{code}_v{id}.csv`, and the library
learns the language straight from that filename on HuggingFace.

To add one, find its YouVersion numeric version id (a full-Bible version works
best), then:

```bash
# 1. fetch and cache it locally
python scripts/build_pivot_caches.py   # edit PIVOTS dict to add your language first

# 2. convert to corpus format
python scripts/prepare_reference_caches.py

# 3. push to HuggingFace
python scripts/push_to_hf.py
```

It's immediately selectable in `africa_corpus.py` once on HuggingFace — nothing
to commit.

---

## Coverage

The dataset covers **1,090 Bible versions across 792+ African languages**,
sourced from [YouVersion](https://www.bible.com) and cross-referenced against
[Glottolog](https://glottolog.org)'s Africa macroarea.

Languages span all major African language families: Niger-Congo, Afro-Asiatic,
Nilo-Saharan, Khoisan, and Austronesian (Madagascar). Total verse records: ~16
million.

---

## Maintainer tooling (building the datasets)

The raw CSVs in `michsethowusu/africa-corpus` were produced by the scripts
below — **regular users do not need to run them.** For maintainers extending
coverage:

- `youversion_parallel_text_builder.py` — automated CSV-driven scraper; reads
  `youversion_africa_versions.csv` and scrapes every version, one at a time.
  Resume-safe: interrupted runs pick up exactly where they left off.
- `youversion_common.py` — shared API helpers (chapter fetcher, text cleaner,
  session pool).
- `scripts/build_pivot_caches.py` — fetches the five pivot/reference Bibles (en/fr/ar/zh/pt)
  into `pivots/`.
- `scripts/prepare_reference_caches.py` — converts `pivots/` into the corpus format
  expected by `africa_corpus.py`.
- `scripts/push_to_hf.py` — incremental sync of `african_bible_parallel_text_datasets/`
  to `michsethowusu/africa-corpus`; only uploads files not already on HF.

Typical workflow for extending coverage:

```bash
# scrape new versions (or re-run after adding rows to the CSV)
python youversion_parallel_text_builder.py youversion_africa_versions.csv

# then push new files to HuggingFace
python scripts/push_to_hf.py --dry-run   # preview what's new
python scripts/push_to_hf.py             # sync to HF
```

---

## Data source

Verse text comes from public **Bible translations**, which are among the best
naturally-occurring sources of sentence-aligned parallel text for low-resource
languages.

> Text was retrieved from [YouVersion](https://www.bible.com) (bible.com).
> Please review YouVersion's terms of service before publishing or
> redistributing derived data.

---

## License

Code in this repository is released under the MIT License. Dataset content is
derived from third-party Bible translations; review the source's terms before
publishing or distributing.

---

## Acknowledgements

Inspired by [ghana-corpus-builder](https://github.com/GhanaNLP/ghana-corpus-builder)
by the [Ghana NLP Community](https://ghananlp.org). Language–macroarea mapping
from [Glottolog](https://glottolog.org). If you use this data in research,
please cite the underlying Bible-translation sources.
