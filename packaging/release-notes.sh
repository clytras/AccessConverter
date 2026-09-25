#!/usr/bin/env bash
# Prints the description of a GitHub release: the version's section of CHANGELOG.md, followed by how to get and check
# the downloads. Fails when CHANGELOG.md has no section for the version, so no release goes out without its notes.
#
#   packaging/release-notes.sh <version> [<tag>]
#
# Links relative to the repository (docs/…) are made absolute at the tag, since a release page can't resolve them.
set -euo pipefail

version=$1
tag=${2:-v$version}
repo=https://github.com/clytras/AccessConverter
image=ghcr.io/clytras/accessconverter
root=$(cd "$(dirname "$0")/.." && pwd)

section=$(awk -v heading="## [$version]" '
    index($0, heading) == 1 { found = 1; next }
    found && /^## \[/ { exit }
    found && /^\[[^]]+\]: / { exit }
    found { print }
' "$root/CHANGELOG.md")

if [[ -z "${section//[[:space:]]/}" ]]; then
    echo "CHANGELOG.md has no section for $version: add \"## [$version] - <date>\" before releasing" >&2
    exit 1
fi

# The section without its leading and trailing blank lines, its repository links made absolute, and its hard-wrapped
# lines joined: a release page, unlike a Markdown file, renders every line break
printf '%s\n' "$section" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}' \
    | sed -E "s#\]\((docs/|README\.md|CHANGELOG\.md|LICENSE)#](${repo}/blob/${tag}/\1#g" \
    | awk '
        /^$/ || /^#/ || /^\|/ { if (line != "") print line; line = ""; print; next }
        /^- / { if (line != "") print line; line = $0; next }
        { sub(/^ +/, ""); line = (line == "" ? $0 : line " " $0) }
        END { if (line != "") print line }
    '

cat << EOF

## Downloads

| | |
| --- | --- |
| **Runtime images, no Java needed** | \`accessconverter-$version-<platform>.zip\` for \`linux-x64\`, \`linux-aarch64\`, \`windows-x64\`, \`macos-x64\`, \`macos-aarch64\`: unzip, then run \`bin/accessconverter\` (\`bin\\accessconverter.cmd\` on Windows). On macOS, clear the download quarantine once: \`xattr -dr com.apple.quarantine accessconverter-$version-macos-*\` |
| **Jar** | \`accessconverter-$version.jar\`, with Java 21 or later: \`java -jar accessconverter-$version.jar --help\` |
| **Container** | \`docker run --rm -u "\$(id -u):\$(id -g)" -v "\$PWD:/data" $image:$version --help\` |

Check a download with \`sha256sum -c SHA256SUMS --ignore-missing\`, or its build provenance with \`gh attestation verify <file> --repo clytras/AccessConverter\`. The [changelog]($repo/blob/$tag/CHANGELOG.md) lists every release.
EOF
