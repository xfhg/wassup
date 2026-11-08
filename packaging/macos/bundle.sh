#!/bin/bash
# Build Wassup.app from a GUI binary on macOS.
#
#   packaging/macos/bundle.sh <binary> <output.app> <version>
#
# Set CODESIGN_IDENTITY to use a Developer ID. Otherwise, use the ad-hoc
# signature required on arm64.
#
# Preserve the supplied size-specific artwork in wassup.icns.
# Info.plist and icon provenance (ICON.md) are next to this script.
set -euo pipefail

binary="$1"
app="$2"
version="$3"
numeric_version="${version%%-*}"
here="$(cd "$(dirname "$0")" && pwd)"

if [[ "$(basename "$app")" != "Wassup.app" || -L "$app" ]]; then
    echo "Output must be a real Wassup.app directory, not a link." >&2
    exit 2
fi
if [[ -e "$app" ]]; then
    bundle_id=$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$app/Contents/Info.plist" 2>/dev/null || true)
    if [[ "$bundle_id" != "local.wassup" ]]; then
        echo "Refusing to replace a bundle that is not Wassup." >&2
        exit 2
    fi
fi
rm -rf -- "$app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"

cp "$binary" "$app/Contents/MacOS/wassup"
chmod 755 "$app/Contents/MacOS/wassup"
sed "s/__VERSION__/$numeric_version/g" "$here/Info.plist" > "$app/Contents/Info.plist"

# Starting in the tray at login: a login item cannot pass --start-hidden, so
# SMAppService registers this agent from the bundle instead.
identifier=local.wassup
mkdir -p "$app/Contents/Library/LaunchAgents"
cp "$here/autostart.plist" "$app/Contents/Library/LaunchAgents/$identifier.autostart.plist"

cp "$here/wassup.icns" "$app/Contents/Resources/wassup.icns"

# The notification plays Wassup's built-in sounds from the bundle by name, so
# Do Not Disturb and Focus silence them (src/notify/macos.rs).
sounds="$here/../../assets/sounds"
cp "$sounds/receive.wav" "$app/Contents/Resources/zapfast-receive.wav"
cp "$sounds/alert.wav" "$app/Contents/Resources/zapfast-alert.wav"

# Attach microphone and camera entitlements for both ad-hoc and Developer ID
# signatures. The private build uses ad-hoc signing by default.
if [ -n "${CODESIGN_IDENTITY:-}" ]; then
    codesign --force --timestamp --options runtime \
        --entitlements "$here/entitlements.plist" --sign "$CODESIGN_IDENTITY" "$app"
else
    codesign --force --entitlements "$here/entitlements.plist" --sign - "$app"
fi
codesign --verify --strict "$app"

echo "$app"
