#!/bin/bash

function convert-chunk-to-logdna-project-format {

  f=$1

  [ -z $f ] && return
  [ -e $f ] || return

  f_temp=$f.temp

  # convert into the format LogDNA needs, preserving the individual line timestamps
  # and applying some dictionary substitution for readbility. ASN numbers that
  # aren't mapped as converted :number --> :"ASnumber"
  records=$(wc -l $f | xargs | cut -f1 -d' ')
  cat $f |
      jq "{ timestamp:(.EdgeEndTimestamp/1000000)|round, line: .|del(.EdgeEndTimestamp)|tojson, file:\"$logdna_app\" }" |
      jq -s -c '{ lines:.}' \
      > $f_temp

  [ -s "$f_temp" ] && cp -f $f_temp $f
  [ -e "$f_temp" ] && rm "$f_temp"
}



## Send a JSON file that is LogDNA-structured, to 1-2 endpoints
function send-chunk-to-logdna {

  f=$1
  [ -z $f ] && return
  [ -s $f ] || return

  >&2 echo "Sending $f contents to LogDNA ingestion endpoint"
  cat $f |
      curl -s "$logdna_endpoint" -u $logdna_ik: -H "Content-Type: application/json; charset=UTF-8" -d @- >&2
  >&2 echo ""
  if [ ! -z "$logdna_endpoint_2nd" ] && [ ! "$logdna_endpoint_2nd" == "null" ] ; then
    >&2 echo "Sending $f contents to 2nd LogDNA ingestion endpoint"
    cat $f |
        curl -s "$logdna_endpoint_2nd" -H "Content-Type: application/json; charset=UTF-8" -d @- >&2
    >&2 echo ""
  fi
  [ -e "$f" ] && rm $f
}


source_file="$1"
logdna_endpoint="$2"
logdna_ik="$3"
logdna_app="$4"
max_send_lines=$5
cos_bucket_root=$6
btoken=$7
logdna_endpoint_2nd=$8

[ -z "$source_file" ] && exit
[ -s "$source_file" ] || exit
[ -z "$logdna_endpoint" ] && exit
[ -z "$logdna_app" ] && logdna_app="cislog"
[ -z "$max_send_lines" ] && max_send_lines=2500
[ 100 -gt $max_send_lines ] && max_send_lines=100


# Ingestion API has a stated limit of 10MB per request, 2KB/line lines  --> 5000/request
records=$(wc -l $source_file | xargs | cut -f1 -d' ')
[ -z "$records" ] && records=0

if [ $records -gt $max_send_lines ] ; then

  split -a 3 -l $max_send_lines $source_file $source_file-logdna-split
  for f in ${source_file}-logdna-split???
  do
    [ -e "$f" ] || break
    convert-chunk-to-logdna-project-format $f & job_pids+=($!)
  done
  wait "${job_pids[@]}"
  for f in ${source_file}-logdna-split???
  do
    [ -e "$f" ] || break
    send-chunk-to-logdna $f
  done
else
  convert-chunk-to-logdna-project-format $source_file
  send-chunk-to-logdna $source_file
fi
>&2 echo "Done"






