# config.py — shared configuration values for all state scrapers

import os
import ssl
import sys
import tempfile
import threading
from pathlib import Path

# --- http ---

# Spoofs a standard browser to avoid 403s from state disclosure sites.
# Update this to match your own browser/OS if you start getting blocked.
# Find your user agent at: https://www.whatismybrowser.com
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)


# --- TLS trust ---------------------------------------------------------
#
# On a network that inspects TLS (Zscaler, Netskope, a proxy
# appliance), every HTTPS response is re-signed by an internal root CA. That
# root lives in the OS trust store, so browsers and curl are happy, but
# `requests` validates against certifi's bundle instead and fails with:
#
#   SSLError: certificate verify failed: unable to get local issuer certificate
#
# The symptom looks like the state's site is broken. It isn't — the same URL
# works fine in the browser, which is the tell.
#
# ca_bundle() fixes this using only the standard library: on Windows,
# ssl.enum_certificates() reads the very same trust store the browser uses, so
# we export those roots to a PEM and hand it to requests. Nothing to install.
#
# Deliberately NOT verify=False. That would silence the error by accepting any
# certificate at all, on a network already known to be intercepting traffic.

_CA_BUNDLE_CACHE = Path(tempfile.gettempdir()) / "state_cf_ca_bundle.pem"

# Windows stores to export.
#
# "ROOT" only, deliberately. The intermediate store ("CA") accumulates years of
# certificates from every product ever installed, and OpenSSL is far stricter
# than Windows' CryptoAPI about what counts as a valid CA — a single cert whose
# basicConstraints extension isn't marked critical makes verification fail with
#   SSLError: Basic Constraints of CA cert not marked critical
# even when the root that actually matters is present and fine. Servers are
# supposed to send their own intermediates anyway, so exporting "CA" buys
# little and risks poisoning the whole bundle.
_WINDOWS_CERT_STORES = ("ROOT",)

# Certs are only usable for HTTPS if they're trusted for server auth.
_SERVER_AUTH_OID = "1.3.6.1.5.5.7.3.1"


def _windows_root_pems() -> list[str]:
    """Export server-auth roots from the Windows trust store as PEM strings.

    Returns [] on any non-Windows platform (enum_certificates doesn't exist)
    or if the store can't be read.
    """
    enum_certificates = getattr(ssl, "enum_certificates", None)
    if enum_certificates is None:
        return []          # not Windows

    pems: list[str] = []
    seen: set[bytes] = set()
    for store in _WINDOWS_CERT_STORES:
        try:
            certs = enum_certificates(store)
        except Exception:
            continue       # store missing or access denied — try the next
        for cert_bytes, encoding, trust in certs:
            if encoding != "x509_asn" or cert_bytes in seen:
                continue
            # `trust` is True (trusted for everything) or a set of purpose OIDs.
            if trust is not True and _SERVER_AUTH_OID not in (trust or ()):
                continue
            try:
                pems.append(ssl.DER_cert_to_PEM_cert(cert_bytes))
                seen.add(cert_bytes)
            except Exception:
                continue   # malformed entry — skip rather than abort
    return pems


def ca_bundle(refresh: bool = False):
    """Return a `verify=` value for requests that trusts the OS trust store.

    Resolution order:
      1. REQUESTS_CA_BUNDLE / SSL_CERT_FILE, if the operator set one — always
         wins, so an explicitly supplied corporate PEM is never second-guessed.
      2. On Windows, a generated bundle of the OS roots plus certifi's, cached
         in the temp dir. Regenerated when missing or refresh=True.
      3. Otherwise True — plain default verification, which is correct on
         macOS/Linux where certifi normally suffices.

    Never returns False. If the OS store can't be read, verification stays on
    and the caller gets a real SSL error rather than a silent downgrade.
    """
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        path = os.environ.get(var)
        if path and Path(path).is_file():
            return path

    if not refresh and _CA_BUNDLE_CACHE.is_file() and _CA_BUNDLE_CACHE.stat().st_size:
        return str(_CA_BUNDLE_CACHE)

    pems = _windows_root_pems()
    if not pems:
        return True        # not Windows, or store unreadable — default behaviour

    # Append certifi's roots so public CAs still validate on a machine whose
    # OS store is narrower than certifi's. certifi ships with requests, so
    # this adds no dependency; skipped silently if it somehow isn't present.
    try:
        import certifi
        pems.append(Path(certifi.where()).read_text(encoding="utf-8"))
    except Exception:
        pass

    try:
        _CA_BUNDLE_CACHE.write_text("\n".join(pems), encoding="utf-8")
        # Prove OpenSSL will actually accept it before handing it out. A
        # bundle that can't be loaded would otherwise fail every request with
        # an opaque "PEM lib" error far from the cause.
        ssl.create_default_context(cafile=str(_CA_BUNDLE_CACHE))
    except (OSError, ssl.SSLError):
        try:
            _CA_BUNDLE_CACHE.unlink()
        except OSError:
            pass
        return True        # unusable — fall back to default verification
    return str(_CA_BUNDLE_CACHE)


def make_ssl_context(verify=None) -> ssl.SSLContext:
    """An SSL context that doesn't reject technically-malformed CA certs.

    **Python 3.13 turned on `VERIFY_X509_STRICT` by default** in
    `ssl.create_default_context()`. Every earlier Python, and every browser,
    leaves it off. Under strict mode OpenSSL rejects a CA certificate whose
    `basicConstraints` extension isn't marked critical:

        SSLError: certificate verify failed: Basic Constraints of CA cert
                  not marked critical

    Corporate TLS-inspection appliances routinely mint exactly such certs, so
    on 3.13 every HTTPS request through one fails while Edge on the same
    machine is perfectly happy. No CA bundle can fix it — the offending cert
    arrives in the chain the proxy presents, not from the trust store.

    Clearing the flag restores the behaviour of Python ≤3.12. It is NOT
    `verify=False`: the chain is still verified against the trust store, still
    checks expiry, hostname and signatures. The only relaxation is one
    encoding-pedantry check about a cert we're going to be handed regardless.
    """
    bundle = ca_bundle() if verify is None else verify
    try:
        ctx = ssl.create_default_context(
            cafile=bundle if isinstance(bundle, str) else None)
    except (ssl.SSLError, OSError):
        # An unreadable or malformed bundle must not take down every request —
        # fall back to the system defaults and let verification proceed.
        ctx = ssl.create_default_context()
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


def tls_adapter(verify=None):
    """A requests HTTPAdapter using make_ssl_context(). Mount on https://."""
    from requests.adapters import HTTPAdapter

    class _RelaxedStrictAdapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            kwargs["ssl_context"] = make_ssl_context(verify)
            return super().init_poolmanager(*args, **kwargs)

        def proxy_manager_for(self, *args, **kwargs):
            kwargs["ssl_context"] = make_ssl_context(verify)
            return super().proxy_manager_for(*args, **kwargs)

    return _RelaxedStrictAdapter()


# Resolved once per process: (verify, relax_strict). None until first probe.
_TLS_STRATEGY: tuple | None = None
_TLS_LOCK = threading.Lock()

# Least-invasive first. Each step gives up one protection, so a machine that
# doesn't need a step never pays for it:
#   1. stock Python — certifi, and 3.13's strict checks left ON
#   2. + OS trust store — needed only where an internal CA signs traffic
#   3. + relaxed strict — needed only where that CA is malformed
_TLS_LADDER = [
    ("stock verification",              lambda: (True, False)),
    ("OS trust store",                  lambda: (ca_bundle(), False)),
    ("relaxed X509_STRICT",             lambda: (True, True)),
    ("OS trust store + relaxed STRICT", lambda: (ca_bundle(), True)),
]


def resolve_tls(url: str, timeout: int = 15, quiet: bool = True) -> tuple:
    """Find the least-invasive TLS config that connects to `url`.

    Probes once per process and caches, so a 12-worker sweep pays for it once.

    The point is that a clean machine should not inherit workarounds a
    corporate one needs. Stock verification is tried first and, on a normal
    network, wins immediately — keeping certifi and Python 3.13's strict
    checks fully intact. Only a machine that actually fails moves down the
    ladder, and only as far as it must.

    If every rung fails (offline, DNS down, blocked), returns the most capable
    config rather than raising, so the real error surfaces from the real
    request with its proper message instead of from a probe.
    """
    global _TLS_STRATEGY
    if _TLS_STRATEGY is not None:
        return _TLS_STRATEGY

    with _TLS_LOCK:
        if _TLS_STRATEGY is not None:      # another thread won the race
            return _TLS_STRATEGY

        import requests
        for label, build in _TLS_LADDER:
            verify, relax = build()
            try:
                session = requests.Session()
                if relax:
                    session.mount("https://", tls_adapter(verify))
                session.get(url, verify=verify, timeout=timeout)
            except requests.exceptions.SSLError:
                continue                   # this rung isn't enough — escalate
            except requests.RequestException:
                break                      # not a TLS problem; stop probing
            if not quiet:
                print(f"[tls] using: {label}")
            _TLS_STRATEGY = (verify, relax)
            return _TLS_STRATEGY

        # Nothing connected. Hand back the most capable rung so the caller's
        # own request produces the real diagnostic.
        _TLS_STRATEGY = (ca_bundle(), True)
        return _TLS_STRATEGY


def diagnose_tls(url: str, timeout: int = 20) -> str | bool | None:
    """Find a `verify=` setting that actually works against `url`, and say so.

    Exists because "which CA bundle does this machine need" is not answerable
    by reasoning — corporate trust stores contain certs OpenSSL rejects but
    Windows accepts, so the only reliable test is to make the request. Tries
    each candidate in turn and returns the first that connects, or None.

    Import-light and side-effect-free apart from writing the cached bundle.
    """
    import requests   # local import: config.py must stay importable without it

    candidates: list[tuple[str, object]] = []

    env = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
    if env:
        candidates.append((f"REQUESTS_CA_BUNDLE={env}", env))

    generated = ca_bundle(refresh=True)
    if generated is not True:
        n = Path(generated).read_text(encoding="utf-8").count("BEGIN CERTIFICATE")
        candidates.append((f"generated OS bundle ({n} certs)", generated))

    try:
        import certifi
        candidates.append((f"certifi only ({certifi.where()})", certifi.where()))
    except Exception:
        pass

    candidates.append(("python default", True))

    strict_default = bool(
        ssl.create_default_context().verify_flags
        & getattr(ssl, "VERIFY_X509_STRICT", 0))

    print(f"Testing TLS against {url}")
    print(f"  python {sys.version.split()[0]}, {ssl.OPENSSL_VERSION}")
    print(f"  VERIFY_X509_STRICT on by default: {strict_default}"
          f"{'   <- Python 3.13+ default; the usual cause' if strict_default else ''}\n")

    winner = None
    for label, verify in candidates:
        try:
            requests.get(url, verify=verify, timeout=timeout)
        except requests.exceptions.SSLError as e:
            msg = str(e)
            for marker in ("certificate verify failed:", "SSLError("):
                if marker in msg:
                    msg = msg.split(marker, 1)[1].strip(" ()'\"")
                    break
            print(f"  [FAIL] {label}\n           {msg[:110]}")
            continue
        except requests.RequestException as e:
            print(f"  [SKIP] {label}\n           non-TLS failure: {type(e).__name__}")
            continue
        print(f"  [ OK ] {label}")
        if winner is None:
            winner = verify

    # Last resort, and the one that fixes the 3.13 strict-mode case: same
    # bundles, but with VERIFY_X509_STRICT cleared.
    if winner is None:
        print()
        for label, verify in candidates:
            try:
                s = requests.Session()
                s.mount("https://", tls_adapter(verify))
                s.get(url, timeout=timeout)
            except requests.RequestException as e:
                print(f"  [FAIL] relaxed X509_STRICT + {label}\n"
                      f"           {type(e).__name__}")
                continue
            print(f"  [ OK ] relaxed X509_STRICT + {label}")
            print("\nThe only thing that changed is VERIFY_X509_STRICT — the chain is\n"
                  "still fully verified. scrapers/new_jersey.py already uses this via\n"
                  "config.tls_adapter(), so just re-run it; no env var needed.")
            return verify

    print()
    if winner is None:
        print("Nothing worked, even with X509_STRICT relaxed. Export your corporate\n"
              "root from Edge (lock icon → Connection is secure → certificate →\n"
              "Details → Copy to File → Base-64 encoded X.509) and set\n"
              "REQUESTS_CA_BUNDLE to it.")
    elif winner is True:
        print("Python's default verification works — no bundle needed.")
    else:
        print(f"Use this:\n  setx REQUESTS_CA_BUNDLE \"{winner}\"\n"
              "then open a NEW terminal (setx doesn't affect the current one).")
    return winner
