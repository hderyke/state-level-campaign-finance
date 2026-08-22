# src/registries/candidates/

Reserved for per-state candidate enrichment registries (e.g. `fl.csv`), the
candidate-side counterpart to `../committees/`. Not yet consumed by
`enrich.py` — nothing here today needs candidate-level enrichment fields
(party/office/election_year already come from the scraper for every state
that has them). Add `{state_abbr}.csv` here if/when a state's candidate
data needs a hand-maintained fallback the way `../committees/` does for
committee-candidate affiliation.
