# Phlower

## Design philosophy

Bloomberg-style nerd tool. Dense, numeric, data-forward UI.
Show actual numeric values (0-100 scores, raw metrics), not icons or simplified visual indicators.
Optimize for information density and scannability.
Monospace numbers, tabular alignment, subtle color heat for magnitude.

## Public repo guidelines

Phlower is open source. Commits, PRs, and code comments must not reference internal infrastructure — no cluster names, pod names, restart counts, specific memory numbers, or deployment details. Describe problems and solutions generically ("large databases", "high-throughput environments") not as deployment incidents ("EU crash-looped 183 times").
