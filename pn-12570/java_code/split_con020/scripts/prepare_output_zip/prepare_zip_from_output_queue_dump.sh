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
aws_region=eu-south-1

bucket_name_and_base_path=s3://mvit-prove-aws-emr-parquet-830192246553-001/zip_test/
sqs_dump_name=dump_pn-paper-event-enrichment-output_2024-09-13T15-44-41-638Z.json

selectedPaId="5b994d4a-0fa8-47ac-9c7b-354f1d44a1ce"

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

  paId=$( aws $aws_command_base_args dynamodb get-item --table-name pn-Notifications \
      --key '{ "iun": {"S": "'${iun}'"} }' | jq -r '.Item.senderPaId.S' )

  echo ""
  if ( [ "$paId" = "$selectedPaId" ] ) then
    outputRequestId=$( echo $sendRequestId \
                | sed -e 's/PREPARE_ANALOG/SEND_ANALOG/' -e 's/PREPARE_SIMPLE_/SEND_SIMPLE_/' \
                | sed -e 's/\.PCRETRY_[0-9][0-9]*//' )
    echo "Transform eventRequestId to timelineRequestId : ${sendRequestId} --> ${outputRequestId}"
    csv_line="${iun};${recIndex};${outputRequestId};${generationTime};${eventTime};${registeredLetterCode};${printedPdf}"
    echo "CSV line: $csv_line"
    echo "$csv_line" >> ${tmp_dir}/zip_content/index.csv


    curl -X GET \
      -H "x-pagopa-safestorage-cx-id: pn-test" \
      ${safestorage_api}safe-storage/v1/files/${printedPdf} \
        | tee > ${tmp_dir}/out.txt

    presigned_url=$( cat ${tmp_dir}/out.txt | jq -r ".download.url")
    echo "Download pdf from ${presigned_url}"
    curl ${presigned_url} > ${tmp_dir}/zip_content/${printedPdf}
  else
    echo "IUN $iun sended by PA-Id: $paId discarded"
  fi

done < <( cat ${tmp_dir}/sqs_dump.json | jq -r '.[] | .Body | fromjson | tojson ')

( cd ${tmp_dir}/zip_content/ && zip -r ../con020.zip * )

zip_end_name=$( echo $sqs_dump_name | sed -e 's/.json$/_con020.zip/' )
aws $aws_command_base_args s3 cp "${tmp_dir}/con020.zip"  "${bucket_name_and_base_path}${zip_end_name}"

