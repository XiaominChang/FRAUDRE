#!/bin/sh -e
set -x

# If no arguments are provided, default to 'src'
dir="${1:-src}"

# Remove unused imports
autoflake --ignore-init-module-imports --in-place -r --remove-all-unused-imports "${dir}"
# Sort imports one per line, so autoflake can remove unused imports
autopep8 --in-place --aggressive --recursive --ignore=E4 "${dir}"
isort --force-single-line-imports "${dir}"
# Format the code with Black
black "${dir}"
# Sort the imports again
isort "${dir}"

# Keep the window open until the user presses Enter
read -p "Press Enter to exit"
