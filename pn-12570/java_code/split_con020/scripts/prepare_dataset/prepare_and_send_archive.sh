#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

api_endpoint=localhost:9090
aws_profile=sso_pn-core-dev
aws_region=eu-central-1


input_csv_path=${script_dir}/input.csv

tmp_dir=${script_dir}/tmp/

bol_file=${tmp_dir}/p7m_content/FLUSSO_STAMPA.bol

mkdir -p ${tmp_dir}
rm -rf ${tmp_dir}
mkdir -p ${tmp_dir}/p7m_content

echo "ORC40018faa|FLUSSOSTAMPA_005_003_25_2501_RS_ORC40018faa.PDZ|Z0090998|PPA|3|RS|D|005|003||BW" > $bol_file

while read -r line
do
  requestId=$( echo $line | sed -e 's/;.*//' )
  registeredLetterCode=$( echo $line | sed -e 's/[^;]*;//' | sed -e 's/;.*//' )
  prodType=$( echo $line | sed -e 's/[^;]*;[^;]*;//' | sed -e 's/;.*//' )
  dateTime=$( echo $line | sed -e 's/[^;]*;[^;]*;[^;]*;//' | sed -e 's/;.*//' )
  rnd=$( uuidgen | tr -d '-' )

  echo "RequestId:$requestId code:$registeredLetterCode type:$prodType  on $dateTime"
  echo "${rnd}.pdf||15376371009|${requestId}|||${registeredLetterCode}||||1|" >> $bol_file

  echo "" > ${tmp_dir}/testo.txt
  echo "RequestId:" >> ${tmp_dir}/testo.txt
  echo $requestId | sed -e 's/IUN.*//' | sed -e 's/^/  /' >> ${tmp_dir}/testo.txt
  echo $requestId | sed -e 's/.*IUN_/        IUN_/' >> ${tmp_dir}/testo.txt

  echo "code:$registeredLetterCode" >> ${tmp_dir}/testo.txt
  echo "type:$prodType" >> ${tmp_dir}/testo.txt
  convert  TEXT:${tmp_dir}/testo.txt ${tmp_dir}/p7m_content/${rnd}.pdf

done < <( cat $input_csv_path | sed 1d )

( cd ${tmp_dir}/p7m_content/ && zip -r ../FLUSSO_STAMPA.PDZ.p7m * )
( cd ${tmp_dir}/ && zip -r ${tmp_dir}/PN_EXTERNAL_LEGAL_FACTS.bin  FLUSSO_STAMPA.PDZ.p7m )

#exit 0
cx=pn-test

get_signed_uri(){
cat << EOF > ${tmp_dir}/signedreq.json
{
  "contentType": "application/octet-stream",
  "documentType": "PN_EXTERNAL_LEGAL_FACTS",
  "status":"SAVED"
}
EOF

   sum=$(cat ${tmp_dir}/PN_EXTERNAL_LEGAL_FACTS.bin| openssl dgst -binary -sha256 | openssl base64 -A)
   echo "- Checksum $sum"

   cmd=$(echo curl -s -H\"x-pagopa-safestorage-cx-id: ${cx}\" -H\"x-api-key: \" -H\"content-type: application/json\" -H\"x-checksum: SHA-256\" -H\"x-checksum-value: ${sum}\" -d@${tmp_dir}/signedreq.json -XPOST http://${api_endpoint}/safe-storage/v1/files )

   echo "Execute:"
   echo $cmd

   resp=$( eval $cmd )
   echo "RISPOSTA"
   echo $resp

   url=$(echo "${resp}" | jq -r '.uploadUrl')
   secret=$(echo "${resp}" | jq -r '.secret')
   key=$(echo "${resp}" | jq -r '.key')
}


get_signed_uri

echo URL:    ${url}
echo Secret: ${secret}
echo Key:    ${key}

curl -XPUT \
    -H"Content-type: application/octet-stream" \
    -H"x-amz-checksum-sha256: ${sum}" \
   --upload-file ${tmp_dir}/PN_EXTERNAL_LEGAL_FACTS.bin  \
    -H"x-amz-meta-secret: ${secret}" \
       ${url}


echo "ARCHIVE FILE LOADED ON SAFESTORAGE KEY ${key}"


echo "Put events to queue"

aws_command_base_args=""
if ( [ ! -z "${aws_profile}" ] ) then
  aws_command_base_args="${aws_command_base_args} --profile $aws_profile"
fi
if ( [ ! -z "${aws_region}" ] ) then
  aws_command_base_args="${aws_command_base_args} --region $aws_region"
fi
echo $aws_command_base_args

queue_url=$(aws $aws_command_base_args sqs get-queue-url --queue-name pn-paper-event-enrichment-input | jq -r '.QueueUrl' )
echo "Queue URL: ${queue_url}"

while read -r line
do
  requestId=$( echo $line | sed -e 's/;.*//' )
  registeredLetterCode=$( echo $line | sed -e 's/[^;]*;//' | sed -e 's/;.*//' )
  prodType=$( echo $line | sed -e 's/[^;]*;[^;]*;//' | sed -e 's/;.*//' )
  dateTime=$( echo $line | sed -e 's/[^;]*;[^;]*;[^;]*;//' | sed -e 's/;.*//' )

cat << EOF > ${tmp_dir}/queue_evt.json
{
   "analogMail":{
      "requestId":"${requestId}",
      "registeredLetterCode":"${registeredLetterCode}",
      "productType":"${prodType}",
      "iun":null,
      "statusCode":"CON020",
      "statusDescription":"Affido conservato",
      "statusDateTime":"${dateTime}",
      "attachments":[
         {
            "id":"0",
            "documentType":"Affido conservato",
            "uri":"safestorage://${key}",
            "sha256":"${sum}",
            "date":"${dateTime}"
         }
      ],
      "clientRequestTimeStamp":"${dateTime}"
   },
   "clientId":"pn-cons-000",
   "eventTimestamp":"${dateTime}"
}
EOF

  eval aws $aws_command_base_args sqs send-message \
      --queue-url ${queue_url} \
      --message-body "'"$( cat ${tmp_dir}/queue_evt.json | jq -r '. | tojson ')"'"

done < <( cat $input_csv_path | sed 1d )

