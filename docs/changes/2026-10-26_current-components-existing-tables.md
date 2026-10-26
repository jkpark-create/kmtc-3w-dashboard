# Latest BSA and B/L in existing dashboard tables

The dashboard previously displayed independently verified BSA and B/L in separate panels while its main tables retained the Booking snapshot. The web consumer now validates and applies current components before initializing the existing shared filters and rendering the existing tables and charts.

- BSA replaces only the complete months covered by its verified component.
- B/L detail is grouped by own identifier, shipper, lane, team, and fiscal week. Its TEU, CM1, freight and CM1-basis TEU must reconcile to every verified lane before publication.
- The source stage retains dictionary-packed shipper detail within the existing 16 MiB component budget. Duplicate identifiers, unknown quantities, changed generations and lane mismatches block publication.
- The consumer replaces actual measures in monthly, weekly and shipper tables; it preserves original Booking and WOS cohort measures and pre-cutover history. Profitability bases are rebuilt from historical and current own-B/L facts. Historical grade metadata is reused only when unambiguous; unclassified shippers remain unclassified.
- BSA-only lanes remain available in shared filters. The header gives separate Booking/WOS, B/L and BSA dates. No independent-query panels are mounted.

Validation: source unit tests, frontend integration tests, all-month/all-grain reconciliation against production components, Booking/WOS conservation, and rendered shared-filter and BL high-profit-tab interactions.

Initial verified component dates: Booking/WOS 20261022; BSA and B/L 20261024. These dates are provenance, not a claim that Booking was refreshed.
