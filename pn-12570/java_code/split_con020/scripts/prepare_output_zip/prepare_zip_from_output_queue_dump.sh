#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

safestorage_api=http://localhost:9090/
aws_profile=sso_pn-core-dev
aws_region=eu-central-1

bucket_name_and_base_path=s3://cf-templates-1rnjss1bg2rvu-eu-central-1/zip_test/
sqs_dump_name=dump_pn-paper-event-enrichment-output_2024-09-13T15-44-41-638Z.json


aws_command_base_args=""
if ( [ ! -z "${aws_profile}" ] ) then
  aws_command_base_args="${aws_command_base_args} --profile $aws_profile"
fi
if ( [ ! -z "${aws_region}" ] ) then
  aws_command_base_args="${aws_command_base_args} --region $aws_region"
fi
echo "AWS commands args $aws_command_base_args"


tmp_dir=${script_dir}/tmp/

mkdir -p ${tmp_dir}
rm -rf ${tmp_dir}
mkdir -p ${tmp_dir}/zip_content

aws $aws_command_base_args s3 cp  "${bucket_name_and_base_path}${sqs_dump_name}" ${tmp_dir}/sqs_dump.json

echo "iun;recIndex;sendRequestId;generationTime;eventTime;registeredLetterCode;printedPdf" > ${tmp_dir}/zip_content/index.csv

while read -r json_line
do
  iun=$( echo $json_line | jq -r '.iun')
  recIndex=$( echo $json_line | jq -r '.recIndex')
  sendRequestId=$( echo $json_line | jq -r '.sendRequestId')
  generationTime=$( echo $json_line | jq -r '.generationTime')
  eventTime=$( echo $json_line | jq -r '.eventTime')
  registeredLetterCode=$( echo $json_line | jq -r '.registeredLetterCode')
  printedPdf=$( echo $json_line | jq -r '.printedPdf')

  csv_line="${iun};${recIndex};${sendRequestId};${generationTime};${eventTime};${registeredLetterCode};${printedPdf}"
  echo "Msg: $csv_line"
  echo "$csv_line" >> ${tmp_dir}/zip_content/index.csv


  curl -X GET \
    -H "x-pagopa-safestorage-cx-id: pn-test" \
    ${safestorage_api}safe-storage/v1/files/${printedPdf} \
      | tee > ${tmp_dir}/out.txt

  presigned_url=$( cat ${tmp_dir}/out.txt | jq -r ".download.url")
  echo "Download pdf from ${presigned_url}"
  curl ${presigned_url} > ${tmp_dir}/zip_content/${printedPdf}

done < <( cat ${tmp_dir}/sqs_dump.json | jq -r '.[] | .Body | fromjson | tojson ')

