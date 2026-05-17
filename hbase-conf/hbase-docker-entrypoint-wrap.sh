#!/usr/bin/env bash
# HariSekhon /entrypoint.sh runs: sed -i 's/zookeeper:2181/localhost:2181/' /hbase/conf/hbase-site.xml
# A read-only bind mount on that path breaks sed (Resource busy). Copy template into place first.
set -euo pipefail
if [[ -f /opt/lakehouse/hbase-site-template.xml ]]; then
  cp -f /opt/lakehouse/hbase-site-template.xml /hbase/conf/hbase-site.xml
fi
exec /entrypoint.sh "$@"
