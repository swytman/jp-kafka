#!/bin/bash

# Check if the version parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

VERSION=$1

# Delete the remote tag
git push --delete origin "$VERSION"

# Delete the local tag
git tag -d "$VERSION"

# Push the tag again
git push origin "$VERSION"

echo "Successfully deleted and re-pushed tag: $VERSION"

