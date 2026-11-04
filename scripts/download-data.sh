#!/usr/bin/env sh
# Downloads the NYC TLC Yellow Taxi monthly Parquet files and the zone lookup into data/.
# Usage: scripts/download-data.sh [--dry-run | --delete] SELECTION...
#   SELECTION: a year (2025), a year range (2011-2026) or a single month (2025-01). With --delete, also: all.
#   --dry-run: only report download size vs free disk.
#   --delete:  delete the selected files to free disk. The pipeline never modifies its input, so re-downloading is safe.
# Safe to re-run: files already present are skipped, unpublished months are reported and skipped.
# Every downloaded file is recorded in data/MANIFEST.tsv (bytes, sha256, UTC time, URL): that is the dataset version.
# --delete keeps MANIFEST.tsv: it is the download history that earlier runs refer to.
set -eu

BASE_URL=https://d37ci6vzurychx.cloudfront.net
DATA_DIR=${DATA_DIR:-data}
TRIPS_DIR=$DATA_DIR/trips
MANIFEST=$DATA_DIR/MANIFEST.tsv
FIRST_YEAR=2009

usage() {
  sed -n '3,6p' "$0" | sed 's/^# //' >&2
  exit 2
}

main() {
  mode=download
  case ${1:-} in
    --dry-run) mode=dry-run && shift ;;
    --delete) mode=delete && shift ;;
  esac
  [ $# -gt 0 ] || usage
  if [ "$mode" = delete ]; then
    delete_selected "$@"
    exit 0
  fi

  selected=$(expand_selections "$@")
  plan "$selected"
  report_plan
  if [ "$mode" = dry-run ]; then
    exit 0
  fi
  check_free_disk
  download_all
}

# --- selection --------------------------------------------------------------------------------------------------------

expand_selections() {
  for sel in "$@"; do
    case $sel in
      [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]) year_range "${sel%-*}" "${sel#*-}" ;;
      [0-9][0-9][0-9][0-9]-[0-9][0-9]) single_month "$sel" ;;
      [0-9][0-9][0-9][0-9]) year_range "$sel" "$sel" ;;
      *) fail "invalid selection '$sel' (expected 2025, 2011-2026 or 2025-01)" ;;
    esac
  done
}

year_range() {
  [ "$1" -ge "$FIRST_YEAR" ] && [ "$1" -le "$2" ] || fail "invalid year range $1-$2 (data starts in $FIRST_YEAR)"
  year=$1
  while [ "$year" -le "$2" ]; do
    for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
      echo "$year-$month"
    done
    year=$((year + 1))
  done
}

single_month() {
  month=${1#*-}
  [ "${1%-*}" -ge "$FIRST_YEAR" ] && [ "$month" -ge 1 ] && [ "$month" -le 12 ] || fail "invalid month $1"
  echo "$1"
}

# --- plan: what is present, what is missing, how big ------------------------------------------------------------------

plan() {
  to_download=""
  need_bytes=0
  present=0
  unpublished=""
  for ym in $1; do
    if [ -f "$(trip_file "$ym")" ]; then
      present=$((present + 1))
      continue
    fi
    bytes=$(remote_size "$(trip_url "$ym")")
    if [ -z "$bytes" ]; then
      unpublished="$unpublished $ym"
    else
      to_download="$to_download $ym"
      need_bytes=$((need_bytes + bytes))
    fi
  done
}

report_plan() {
  echo "Already present: $present file(s)"
  # shellcheck disable=SC2086 # word splitting is the point: count the months
  echo "To download:     $(count $to_download) file(s), $(human "$need_bytes")"
  echo "Free disk:       $(human "$(free_bytes)")"
  if [ -n "$unpublished" ]; then
    echo "Not published (skipped):$unpublished"
  fi
}

check_free_disk() {
  # Keep 2 GiB of headroom for Spark shuffle/spill and outputs.
  if [ $((need_bytes + 2147483648)) -gt "$(free_bytes)" ]; then
    fail "not enough disk space: need $(human "$need_bytes") + 2 GiB headroom. Choose fewer YEARS."
  fi
}

# --- download ---------------------------------------------------------------------------------------------------------

download_all() {
  mkdir -p "$TRIPS_DIR"
  [ -f "$MANIFEST" ] || printf 'file\tbytes\tsha256\tdownloaded_utc\turl\n' >"$MANIFEST"

  zones=$DATA_DIR/taxi_zone_lookup.csv
  [ -f "$zones" ] || fetch "$BASE_URL/misc/taxi_zone_lookup.csv" "$zones"
  for ym in $to_download; do
    fetch "$(trip_url "$ym")" "$(trip_file "$ym")"
  done
  echo "Done. Dataset in $DATA_DIR/, versions in $MANIFEST"
}

fetch() {
  echo "Downloading $(basename "$2")"
  # Download to .part and rename, so an interrupted run never leaves a truncated file that looks complete.
  if ! curl -fL --retry 3 --retry-delay 2 --progress-bar -o "$2.part" "$1"; then
    rm -f "$2.part"
    fail "download failed: $1. Re-run to resume; on HTTP 403 the CDN may be rate-limiting, so wait a few minutes first."
  fi
  mv "$2.part" "$2"
  printf '%s\t%s\t%s\t%s\t%s\n' "$2" "$(wc -c <"$2" | tr -d ' ')" "$(sha256 "$2")" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >>"$MANIFEST"
}

# --- delete -----------------------------------------------------------------------------------------------------------

delete_selected() {
  if [ "$1" = all ]; then
    # Delete only what this script creates, never the whole directory (DATA_DIR can be overridden).
    files=$(find "$TRIPS_DIR" -name 'yellow_tripdata_*.parquet*' 2>/dev/null; ls "$DATA_DIR/taxi_zone_lookup.csv" 2>/dev/null || true)
  else
    selected=$(expand_selections "$@")
    files=$(for ym in $selected; do [ ! -f "$(trip_file "$ym")" ] || trip_file "$ym"; done)
  fi
  freed=0
  for file in $files; do
    freed=$((freed + $(wc -c <"$file")))
    rm -f "$file"
  done
  # shellcheck disable=SC2086 # word splitting is the point: count the files
  echo "Deleted $(count $files) file(s), freed $(human "$freed"). Re-download any time with: make data YEARS=..."
}

# --- helpers ----------------------------------------------------------------------------------------------------------

trip_url() { echo "$BASE_URL/trip-data/yellow_tripdata_$1.parquet"; }

trip_file() { echo "$TRIPS_DIR/yellow_tripdata_$1.parquet"; }

# Content-Length of a published file, or empty if it isn't published (the CDN answers 403).
# curl prints the headers of error responses too, so only trust the size when the final status is 200.
remote_size() {
  curl -sIL "$1" | parse_size || true
}

parse_size() {
  tr -d '\r' | awk '
    /^HTTP\// { status = $2; size = "" }
    tolower($1) == "content-length:" { size = $2 }
    END { if (status == 200) print size }'
}

free_bytes() {
  df -Pk . | awk 'NR == 2 { printf "%.0f", $4 * 1024 }'
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

human() {
  awk -v b="$1" 'BEGIN { split("B KiB MiB GiB TiB", u); i = 1; while (b >= 1024 && i < 5) { b /= 1024; i++ } printf "%.1f %s", b, u[i] }'
}

count() { echo $#; }

fail() {
  echo "download-data: $*" >&2
  exit 1
}

main "$@"
