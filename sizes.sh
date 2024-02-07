#!/bin/bash

# Check if directory argument was provided, otherwise use current directory  
if [ $# -eq 0 ]; then
  DIRECTORY="./"
  echo "No directory passed in, using current directory."
else
  echo "Using Directory: $1"
  DIRECTORY="$1"
fi

# Check if directory exists
if [ ! -d "$DIRECTORY" ]; then
  echo "Directory $DIRECTORY does not exist."
  exit 1  
fi

# Loop through files and output size and lines
for file in "$DIRECTORY"/*; do
  if [ -f "$file" ]; then
    lines=$(($(wc -l < "$file") - 1))  
    bytes=$(ls -l "$file" | awk '{print $5}')
    size=$(echo "scale=2; $bytes/1048576" | bc | awk '{printf "%.2f MB\n", $0}')
    printf "%-50s %10s %10s lines\n" "$file" "$size" "$lines" 
  fi
done

