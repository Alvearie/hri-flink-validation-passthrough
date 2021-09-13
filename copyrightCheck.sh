#!/bin/bash

files=$(find src test . -name "*.java" -or -name "*.rb" -or -name "*.sh")

for file in $files; do
  #echo $file
  if ! head -n 5 $file | grep -qE '(Copyright IBM Corp)|(MockGen)'; then
   echo $file
  fi
done
