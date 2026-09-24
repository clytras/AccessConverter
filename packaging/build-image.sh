#!/usr/bin/env bash
# Builds the self-contained runtime image for this machine's platform (02, Packaging): a jlink'd Java runtime with
# only the modules AccessConverter uses, the shaded jar and a launcher, zipped as
# accessconverter-<version>-<platform>.zip. Users need no Java installed.
#
#   packaging/build-image.sh <shaded jar> <version> <platform> [<output dir>]
#
# <platform> is one of linux-x64, linux-aarch64, windows-x64, macos-x64, macos-aarch64. jlink comes from JAVA_HOME
# (or the PATH), which must be a JDK 21 with its jmods for this platform.
set -euo pipefail

jar=$1
version=$2
platform=$3
out=${4:-target/dist}

# What the jar needs (jdeps: java.base, java.naming, java.scripting, java.sql, which brings java.logging), plus:
# - jdk.charsets: the Access 97 code pages outside java.base on some platforms (874, 949, 950, 1258 on Linux);
#   a runtime without them can't decode a Thai, Korean, Chinese or Vietnamese Access 97 database
# - jdk.crypto.ec: elliptic-curve TLS, for verify --jdbc-url against a server that requires it
# - java.management, java.security.jgss, java.security.sasl, jdk.net: what MySQL Connector/J and MariaDB
#   Connector/J need, since verify --jdbc-driver loads the user's driver into this runtime
modules=java.base,java.logging,java.naming,java.scripting,java.sql,jdk.charsets,jdk.crypto.ec,java.management,java.security.jgss,java.security.sasl,jdk.net

case "$platform" in
    linux-x64 | linux-aarch64 | windows-x64 | macos-x64 | macos-aarch64) ;;
    *) echo "unknown platform: $platform" >&2; exit 64 ;;
esac

if [[ -n "${JAVA_HOME:-}" ]]; then
    jlink="$JAVA_HOME/bin/jlink"
else
    jlink=jlink
fi
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
name="accessconverter-$version-$platform"
work="$out/$name"

rm -rf "$work" "$out/$name.zip"
mkdir -p "$work/bin" "$work/lib"

"$jlink" \
    --add-modules "$modules" \
    --output "$work/runtime" \
    --strip-debug \
    --no-header-files \
    --no-man-pages \
    --compress=zip-6 \
    --generate-cds-archive

cp "$jar" "$work/lib/accessconverter.jar"
cp "$root/LICENSE" "$root/NOTICE" "$root/README.md" "$work/"
cp -r "$here/licenses" "$work/licenses"
if [[ "$platform" == windows-* ]]; then
    cp "$here/launcher/accessconverter.cmd" "$work/bin/"
else
    cp "$here/launcher/accessconverter" "$work/bin/"
    chmod 755 "$work/bin/accessconverter"
fi

# zip keeps the executable bits and symbolic links the Unix images need; Windows has no zip, but 7-Zip on the
# GitHub runners and PowerShell everywhere
if command -v zip > /dev/null; then
    (cd "$out" && zip -qry "$name.zip" "$name")
elif command -v 7z > /dev/null; then
    (cd "$out" && 7z a -tzip -bso0 "$name.zip" "$name")
else
    (cd "$out" && powershell -NoProfile -Command "Compress-Archive -Path '$name' -DestinationPath '$name.zip'")
fi
echo "$out/$name.zip"
