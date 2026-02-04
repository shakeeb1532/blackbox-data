# Notifications

Blackbox can notify on verification failures via webhooks.

## Configure webhooks
```bash
export BLACKBOX_PRO_SLACK_WEBHOOK="https://hooks.slack.com/..."
export BLACKBOX_PRO_TEAMS_WEBHOOK="https://outlook.office.com/webhook/..."
export BLACKBOX_PRO_PAGERDUTY_WEBHOOK="https://events.pagerduty.com/v2/enqueue"
```

When `/verify` detects tampering, Blackbox sends a notification.
