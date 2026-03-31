#!/usr/bin/env bash
set -x

# Check if a folder is provided as an argument, default to 'src' if not
FOLDER=${1:-src}

EXIT_STATUS=0

isort --check-only "$FOLDER" || EXIT_STATUS=$?
pylint "$FOLDER" || EXIT_STATUS=$?

# Keep the window open until the user presses Enter
read -p "Press Enter to exit"

exit $EXIT_STATUS

