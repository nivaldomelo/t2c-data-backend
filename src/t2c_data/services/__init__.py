"""Compatibility bridge package for legacy service imports.

New backend code should prefer importing from `app.features.*` directly.

Use `app.services.*` only when:
- preserving compatibility with older modules or scripts
- the module still owns an external integration client that has not been moved

The canonical separation for new code is:
- catalog read: `app.features.catalog.*`
- metadata mutation: `app.features.catalog.*`, `app.features.tags.*`, `app.features.glossary.*`
- operations: `app.features.platform.*`, `app.features.ingestion.*`, `app.features.datasource.*`
- governance: `app.features.governance.*`, `app.features.stewardship.*`, `app.features.certification.*`
"""
