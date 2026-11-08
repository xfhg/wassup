<p align="center">
  <img src="assets/brand/wassup-readme.png" alt="Wassup" width="256" height="256">
</p>

<h1 align="center">Wassup</h1>

A native WhatsApp client for macOS, written in Rust, that preserves deleted
messages. Based on [ZapFast](https://github.com/crmne/zapfast) 0.21.1.

- **Deleted messages:** read retained originals in the app, grouped by sender.
- **Encrypted archive:** messages, revisions, and attachments in `~/Wassup`,
  unlocked automatically through macOS Keychain.
- **Export on request:** save readable transcripts or attachments when needed.
  Exports are plaintext.
- **Independent profile:** separate WhatsApp pairing; runs after the window closes.

## Limits

Capture requires Wassup to be running and connected, with the Mac awake.
Only received messages and completed downloads are protected. View-once
media, calls, statuses, channels, and historical backfill are excluded.
Deleted and exports show only known, unlocked chats.

**Backups need both `~/Wassup` and its Keychain keys.** Recovery-key export is
not available yet. This is an unofficial client and can risk account suspension.

## Build

Apple Silicon macOS requires the pinned [Rust toolchain](rust-toolchain.toml),
Xcode Command Line Tools, CMake, and Python 3.11+.

```sh
scripts/build-wassup-macos.sh
```

Creates `dist/Wassup.app`. Install and launch it manually.

MIT licensed. See [LICENSE](LICENSE) and [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
