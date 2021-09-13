#!/usr/bin/env bash

# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

echo 'Run Service Tests'
rspec test/nightly --tag ~@broken --format documentation --format RspecJunitFormatter --out nightlytest.xml
