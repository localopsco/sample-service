#!/bin/bash

# Set the file name
FILE="timestamp.txt"

# Write the current timestamp to the file
date +"%Y-%m-%d %H:%M:%S" > "$FILE"

# Add the file to git staging
git add "$FILE"

# Commit with a message including the timestamp
git commit -m "Update timestamp: $(date +"%Y-%m-%d %H:%M:%S")"

# Push to the current branch
git push
