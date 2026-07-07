# config.py — shared configuration values for all state scrapers

# --- http ---

# Spoofs a standard browser to avoid 403s from state disclosure sites.
# Update this to match your own browser/OS if you start getting blocked.
# Find your user agent at: https://www.whatismybrowser.com
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
