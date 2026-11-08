# Wassup macOS icon

Supplied by the owner in `W-macOS-AppIcon-Teal-Corrected.zip` on 2026-11-09. These files are
copied unchanged from the archive; no recoloring, resizing, or recompression
was applied.

| Repository file | Source member | SHA-256 |
| --- | --- | --- |
| `icon-1024.png` | `macOS/WAppIcon.iconset/icon_512x512@2x.png` | `0ed80ec8bcc98ea607f3c5a93d9860e498627acc71583ff20cd62cf660743384` |
| `wassup.icns` | `macOS/WAppIcon.icns` | `15ec9e48496a3f03a2a3bd2781db3a06e81c9ce71f44028a4ff7a9044e0f8712` |
| `assets/brand/wassup-readme.png` (repository root) | `macOS/WAppIcon.iconset/icon_256x256.png` | `bccf0bb84e137ae0089b3aef901a936e9ff6992ff506d8d43eb41b5e45fee077` |

The app PNG is 1024 × 1024, 8-bit RGBA. The README PNG is the supplied
256 × 256 rendition, centered at its native size in the README header.
The supplied ICNS contains the standard
macOS icon sizes, including Retina variants. `bundle.sh` copies it directly
to `Contents/Resources/wassup.icns`, as named by `Info.plist`.

`src/branding.rs` embeds the PNG for the connecting, linking, lock, Settings,
and About screens, and native window icons. `src/main.rs` also embeds it for an
unbundled macOS process's Dock icon, and `src/notify.rs` uses it for the
notification adapter. Runtime rendering resizes the supplied PNG without
recoloring it. Rebuild after replacing the PNG, then package and sign the
bundle. The bundled macOS Dock uses ICNS.

The macOS menu-bar template and empty-chat watermark use a simple rounded W
from `assets/brand/wassup-w.svg`. This monochrome companion stays legible at
small sizes and follows the system menu-bar color. Unread badges are retained.

The obsolete upstream SVG and installer artwork were removed during the
private Mac cleanup. The supplied PNG and ICNS are the application artwork.

`packaging/windows/zapfast.ico` is a format conversion of the same supplied
PNG, retaining transparency at 16, 24, 32, 48, 64, 128, and 256 pixels. It
keeps the conditional Windows resource build valid; no Windows installer or
platform qualification is included in this private Mac build.

The retained Windows resource filename follows the upstream build interface;
its pixels use the current Wassup artwork.
