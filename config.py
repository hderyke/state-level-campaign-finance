# config.py — shared configuration values for all state scrapers

# --- http ---

# Spoofs a standard browser to avoid 403s from state disclosure sites.
# Update if requests start getting blocked (use a current Chrome UA string).
USER_AGENT = (
# change this to whatever browser you are using for scraping
)
