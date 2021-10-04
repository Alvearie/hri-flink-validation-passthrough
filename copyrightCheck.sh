#!/bin/bash

# (C) Copyright IBM Corp. 2020
#
# SPDX-License-Identifier: Apache-2.0

files=$(find . -name "*.java" -or -name "*.rb" -or -name "*.sh")

rtn=0

for file in $files; do
  #echo $file
  if ! head -n 5 $file | grep -qE '(Copyright IBM Corp)|(MockGen)'; then
   rtn=1
   echo $file
  fi
done

if [ $rtn -ne 0 ]; then
  echo "Found files without copyright, exiting."
  exit 1
fi