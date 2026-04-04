---
name: slack-formatting
description: Format messages for Slack using mrkdwn syntax. Use when responding to Slack channels (folder starts with "slack_" or JID contains slack identifiers).
---

# Slack Message Formatting (mrkdwn)

When responding to Slack channels, use Slack's mrkdwn syntax instead of standard Markdown.

## How to detect Slack context

Check your group folder name or workspace path:
- Folder starts with `slack_` (e.g., `slack_engineering`, `slack_general`)
- Or check `/workspace/group/` path for `slack_` prefix

## Formatting reference

### Text styles

| Style | Syntax | Example |
|-------|--------|---------|
| Bold | `*text*` | *bold text* |
| Italic | `_text_` | _italic text_ |
| Strikethrough | `~text~` | ~strikethrough~ |
| Code (inline) | `` `code` `` | `inline code` |
| Code block | ` ```code``` ` | Multi-line code |

### Links and mentions

```
<https://example.com|Link text>     # Named link
<https://example.com>                # Auto-linked URL
<@U1234567890>                       # Mention user by ID
<#C1234567890>                       # Mention channel by ID
<!here>                              # @here
<!channel>                           # @channel
```

### Lists

Slack supports simple bullet lists but NOT numbered lists:

```
• First item
• Second item
• Third item
```

Use `•` (bullet character) or `- ` or `* ` for bullets.

### Block quotes

```
> This is a block quote
> It can span multiple lines
```

### Emoji

Use standard emoji shortcodes: `:white_check_mark:`, `:x:`, `:rocket:`, `:tada:`

## What NOT to use

- **NO** `##` headings (use `*Bold text*` for headers instead)
- **NO** `**double asterisks**` for bold (use `*single asterisks*`)
- **NO** `[text](url)` links (use `<url|text>` instead)
- **NO** `1.` numbered lists (use bullets with numbers: `• 1. First`)
- **NO** tables (use code blocks or plain text alignment)
- **NO** `---` horizontal rules

## Example message

```
*Daily Standup Summary*

_March 21, 2026_

• *Completed:* Fixed authentication bug in login flow
• *In Progress:* Building new dashboard widgets
• *Blocked:* Waiting on API access from DevOps

> Next sync: Monday 10am

:white_check_mark: All tests passing | <https://ci.example.com/builds/123|View Build>
```

## Threading

Responses default to threading (replying in the same thread as the triggering message).

To send a reply at the *channel level* instead (not in a thread), include `<no-thread>` anywhere in your response — it will be stripped before posting. Use this sparingly, e.g. for announcements that should be visible to the whole channel rather than buried in a thread.

```
<no-thread>
This reply goes to the channel, not a thread.
```

## Quick rules

1. Use `*bold*` not `**bold**`
2. Use `<url|text>` not `[text](url)`
3. Use `•` bullets, avoid numbered lists
4. Use `:emoji:` shortcodes
5. Quote blocks with `>`
6. Skip headings — use bold text instead
7. Respond in threads by default; use `<no-thread>` only when channel-level visibility matters
