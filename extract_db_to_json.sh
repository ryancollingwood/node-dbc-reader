#!/bin/bash

for file in data/dbc/*.dbc; do
  filename=$(basename -- "$file")
  name="${filename%.*}"
  lower_name=$(echo "$name" | tr '[:upper:]' '[:lower:]')
  npm run start "$name" | tail -n +6 > "json/${lower_name}_dbc.json"
done