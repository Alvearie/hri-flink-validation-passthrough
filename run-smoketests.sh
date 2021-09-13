#!/usr/bin/env bash

# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

passing=0
failing=0
output=""

echo 'Run Smoke Tests'

FLINK_CREDENTIALS_RAW="$OIDC_HRI_DATA_INTEGRATOR_CLIENT_ID:$OIDC_HRI_DATA_INTEGRATOR_CLIENT_SECRET"
FLINK_CREDENTIALS=$(printf $FLINK_CREDENTIALS_RAW | base64 | tr -d '\n')
FLINK_TOKEN_RESPONSE=$(curl --request POST "$APPID_URL/oauth/v4/$APPID_TENANT/token" --header "Authorization: Basic $FLINK_CREDENTIALS" --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=client_credentials' --data-urlencode "audience=$APPID_FLINK_AUDIENCE")
FLINK_TOKEN=$(printf $FLINK_TOKEN_RESPONSE | jq .access_token | awk 'gsub(/"/, "")')
FLINK_OVERVIEW_STATUS=$(curl --write-out "%{http_code}\n" --silent --output /dev/null "$FLINK_URL/overview" --header "Authorization: Bearer $FLINK_TOKEN")

if [ $FLINK_OVERVIEW_STATUS -eq 200 ]; then
  passing=$((passing+1))
  failure='/>'
else
  failing=$((failing+1))
  FLINK_API_ERROR=$(curl "$FLINK_URL/overview" --header "Authorization: Bearer $FLINK_TOKEN")
  failure="><failure message=\"Expected Flink API overview status to return code 200\" type=\"FAILURE\">$FLINK_API_ERROR</failure></testcase>"
fi
output="$output\n<testcase classname=\"Flink-API\" name=\"GET Overview\" time=\"0\"${failure}"

echo
echo "----------------"
echo "Final Results"
echo "----------------"
echo "PASSING: $passing"
echo "FAILING: $failing"
total=$(($passing + $failing))

date=`date`
header="<testsuite name=\"Smoke tests\" tests=\"$total\" failures=\"$failing\" errors=\"$failing\" skipped=\"0\" timestamp=\"${date}\" time=\"0\">"
footer="</testsuite>"

filename="smoketests.xml"
cat << EOF > $filename
$header
$output
$footer
EOF

if [ $failing -gt 0 ]; then
  exit 1
fi
