#!/bin/bash
for f in /opt/rejected/bin/start-*sh
do
  exec $f
done
/opt/rejected/bin/setaffinity.sh
