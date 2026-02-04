# SIEM Mapping (CEF)

Blackbox can export audit events as JSONL or CEF.

## Export endpoints
```
GET /siem?format=jsonl
GET /siem?format=cef
```

## CEF Field Mapping
| CEF Field | Source |
| --- | --- |
| signature | `event` |
| name | `path` |
| severity | derived from HTTP status |
| `path` | request path |
| `method` | HTTP method |
| `status` | HTTP status |
| `role` | token role |
| `token_id` | hashed token ID |
| `ip` | client IP |
| `user_agent` | user agent |
| `duration_ms` | request latency |
| `detail` | error detail (auth failures) |

## Example CEF
```
CEF:0|Blackbox|DataPro|1.0|request|/report|3|path=/report method=GET status=200 role=admin token_id=abc123 ip=127.0.0.1 duration_ms=12.2
```
