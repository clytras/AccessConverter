#!/usr/bin/env bash
# Smoke test of a release build, run where no Java is installed or none is used (02, acceptance criteria):
#
#   packaging/smoke-test.sh <unpacked image directory> <work directory>
#   packaging/smoke-test.sh docker:<image> <work directory>
#
# - every Access 97 code page is available (a missing jdk.charsets makes --charset a usage error, exit 64)
# - the Greek Access 97 fixture converts to every target and its text comes out exactly
# - its encoded, password-protected copy opens (the Jet 3 codec)
# - three modern databases convert and verify: attachments and multi-value columns (Access 2010), Date/Time
#   Extended (Access 2019) and Agile encryption (BouncyCastle), pinned by commit and SHA-256 like the test corpus
set -euo pipefail

subject=$1
work=$2
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$work"
work=$(cd "$work" && pwd)

if [[ "$subject" == docker:* ]]; then
    image=${subject#docker:}
    # Git Bash on Windows: a Windows path for the mount, and no rewriting of /data into C:/Program Files/Git/data
    host=$(cd "$work" && (pwd -W 2> /dev/null || pwd))
    ac() {
        MSYS_NO_PATHCONV=1 docker run --rm -u "$(id -u):$(id -g)" -e ACCESSCONVERTER_PASSWORD -v "$host:/data" \
            "$image" "$@"
    }
elif [[ -f "$subject/bin/accessconverter.cmd" ]]; then
    launcher="$(cd "$subject" && pwd)/bin/accessconverter.cmd"
    ac() { (cd "$work" && cmd //c "$launcher" "$@"); }
else
    launcher="$(cd "$subject" && pwd)/bin/accessconverter"
    ac() { (cd "$work" && "$launcher" "$@"); }
fi

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# 0 is success and 1 success with warnings; anything else fails the smoke test
run() {
    local status=0
    ac "$@" > "$work/last.out" 2>&1 || status=$?
    if ((status > 1)); then
        cat "$work/last.out" >&2
        fail "exit $status: accessconverter $*"
    fi
    echo "ok (exit $status): accessconverter $*"
}

sha256() {
    if command -v sha256sum > /dev/null; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

fetch() {
    local url=$1 file=$2 expected=$3
    if [[ ! -f "$work/$file" ]]; then
        curl -fsSL --retry 3 -o "$work/$file" "$url"
    fi
    [[ "$(sha256 "$work/$file")" == "$expected" ]] || fail "checksum of $file"
}

cp "$root/src/test/resources/fixtures/access97/gr97.mdb" "$root/src/test/resources/fixtures/access97/gr97enc.mdb" "$work/"
rm -rf "$work"/out-*

ac --version | tee "$work/version.txt"

for charset in x-windows-874 windows-31j GBK x-windows-949 x-windows-950 windows-1250 windows-1251 windows-1252 \
    windows-1253 windows-1254 windows-1255 windows-1256 windows-1257 windows-1258 IBM437 IBM850 IBM852 IBM866; do
    run inspect gr97.mdb --charset "$charset" -o "out-inspect-$charset.txt"
done

run convert gr97.mdb --to json -o out-gr97.json --verify
greek='Ελληνικά: ΑΒΓΔΕΖΗΘ αβγδεζηθ ςΆΈΉΊΌΎΏ'
grep -qF "$greek" "$work/out-gr97.json" || fail "the Greek text of gr97.mdb didn't come out exactly"
run convert gr97.mdb --to sqlite -o out-gr97.sqlite3 --verify
run convert gr97.mdb --to mysql -o out-gr97.sql
grep -qF "$greek" "$work/out-gr97.sql" || fail "the Greek text of gr97.mdb isn't in the MySQL dump"
ACCESSCONVERTER_PASSWORD=gr97pass run convert gr97enc.mdb --to json -o out-gr97enc.json --verify
grep -qF "$greek" "$work/out-gr97enc.json" || fail "the Greek text of gr97enc.mdb didn't come out exactly"

jackcess=https://raw.githubusercontent.com/spannm/jackcess/29610dfd8a13e27bc1aa70ec1e38586bff3633b2/src/test/resources/data
mdbreader=https://raw.githubusercontent.com/andipaetzold/mdb-reader/3dc637a9732db79a3bf67506b82a4f83cf07e6a9
fetch "$jackcess/V2010/complexDataV2010.accdb" complexDataV2010.accdb \
    c9707ec54fd4f712c74ee3638963ad203fc60340f5bb7ad0c86bc2878a91e6df
fetch "$jackcess/V2019/extDateV2019.accdb" extDateV2019.accdb \
    bb6d7af8e5275c1143b5f68447b2f96bd11e460fbec6856de7b3e339ccb53cf6
fetch "$mdbreader/test/encryption/data/office-agile-4.2.accdb" office-agile-4.2.accdb \
    bbabd68b6cd9c581348fe1c967a9e55bdd3cde893229c83c2198d822fd4069d2

for db in complexDataV2010 extDateV2019; do
    run convert "$db.accdb" --to sqlite -o "out-$db.sqlite3" --ole-extract --verify
    run convert "$db.accdb" --to json -o "out-$db.json" --binary files --verify
    run convert "$db.accdb" --to mariadb -o "out-$db.sql"
done
ACCESSCONVERTER_PASSWORD=password run convert office-agile-4.2.accdb --to sqlite -o out-agile.sqlite3 --verify

echo "smoke test passed: $subject"
