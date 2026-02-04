# Compliance Mapping (SOC 2 / ISO 27001)

This is a lightweight mapping to help security and compliance teams understand where
Blackbox Data fits. It is not a certification.

## SOC 2 (Security + Availability)
- **CC6.1 / CC6.6 (Logical Access)**  
  Token auth + role/tenant scoping + audit logs.
- **CC7.2 (Monitoring)**  
  SIEM export (JSONL/CEF) + usage metrics.
- **CC7.3 (Detection / Response)**  
  Verify‑fail notifications + evidence bundles.
- **CC8.1 (Change Management)**  
  Step‑level diffs + integrity chain for pipeline changes.

## ISO 27001 (Annex A)
- **A.5.15 Logging**  
  Audit log export + SIEM mapping.
- **A.5.20 Monitoring Activities**  
  Usage dashboard + verification events.
- **A.5.28 Secure Coding / Change Control**  
  Tamper‑evident evidence chain and verification.
- **A.5.30 Information Deletion**  
  Metadata‑only storage by default reduces data exposure.

## Notes
- Blackbox does not encrypt data at rest; use encrypted volumes if required.
- Evidence bundle signing is HMAC‑based in v1.0.
