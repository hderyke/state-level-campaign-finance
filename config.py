# config.py — shared configuration values for all state scrapers

# --- http ---

# Spoofs a standard browser to avoid 403s from state disclosure sites.
# Update if requests start getting blocked (use a current Chrome UA string).
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
