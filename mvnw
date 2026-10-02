#!/bin/sh
# Minimal Maven wrapper: downloads the Maven distribution named in
# .mvn/wrapper/maven-wrapper.properties on first use, then delegates to it.
set -e
BASE_DIR=$(cd "$(dirname "$0")" && pwd)
URL=$(sed -n 's/^distributionUrl=//p' "$BASE_DIR/.mvn/wrapper/maven-wrapper.properties")
DIST_NAME=$(basename "$URL" -bin.zip)
DIST_DIR="${MAVEN_USER_HOME:-$HOME/.m2}/wrapper/dists/$DIST_NAME"
if [ ! -x "$DIST_DIR/$DIST_NAME/bin/mvn" ]; then
  echo "Downloading $URL" >&2
  mkdir -p "$DIST_DIR"
  TMP_ZIP=$(mktemp)
  curl -fsSL "$URL" -o "$TMP_ZIP"
  unzip -q -o "$TMP_ZIP" -d "$DIST_DIR"
  rm -f "$TMP_ZIP"
fi
exec "$DIST_DIR/$DIST_NAME/bin/mvn" "$@"
