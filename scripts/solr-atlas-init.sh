#!/usr/bin/env bash
# Membuat core Solr yang dibutuhkan JanusGraph/Atlas: vertex_index, edge_index, fulltext_index
# Konfigurasi: solr/atlas-config (Apache Atlas 2.3.0 distro/src/conf/solr)
# Catatan: image solr resmi tidak menyertakan curl — gunakan wget.
set -euo pipefail

SOLR_URL="${SOLR_URL:-http://solr:8983}"
ATLAS_TEMPLATE="${ATLAS_SOLR_TEMPLATE:-/atlas-solr}"
DATA_ROOT="${SOLR_DATA_ROOT:-/var/solr/data}"
CORES=(vertex_index edge_index fulltext_index)

echo "Menunggu Solr di ${SOLR_URL} ..."
ready=0
for _ in {1..90}; do
  if wget -q -T 5 -O /dev/null "${SOLR_URL}/solr/admin/info/system?wt=json" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "${ready}" -ne 1 ]]; then
  echo "ERROR: Solr tidak merespon setelah ~3 menit: ${SOLR_URL}" >&2
  exit 1
fi

list_cores_json() {
  wget -q -T 15 -O- "${SOLR_URL}/solr/admin/cores?action=LIST&wt=json"
}

core_already_exists_response() {
  local f="$1"
  grep -qiE 'already exists|duplicate|is already defined|Core with name' "${f}" 2>/dev/null
}

for core in "${CORES[@]}"; do
  json="$(list_cores_json 2>/dev/null || true)"
  if echo "${json}" | grep -qF "\"${core}\""; then
    echo "Core '${core}' sudah ada (LIST), lewati."
    continue
  fi

  status_json="$(wget -q -T 15 -O- "${SOLR_URL}/solr/admin/cores?action=STATUS&core=${core}&wt=json" 2>/dev/null || true)"
  if echo "${status_json}" | grep -qE "\"name\"[[:space:]]*:[[:space:]]*\"${core}\""; then
    echo "Core '${core}' sudah ada (STATUS), lewati."
    continue
  fi

  inst="${DATA_ROOT}/${core}"
  echo "Membuat core '${core}' di ${inst} ..."
  rm -rf "${inst}"
  mkdir -p "${inst}/conf" "${inst}/data"
  cp -a "${ATLAS_TEMPLATE}/." "${inst}/conf/"
  chown -R 8983:8983 "${inst}" 2>/dev/null || true

  resp_file="/tmp/solr-create-${core}.txt"
  if ! wget -q -T 30 -O "${resp_file}" --post-data="action=CREATE&name=${core}&instanceDir=${inst}&wt=json" \
    "${SOLR_URL}/solr/admin/cores" 2>/dev/null; then
    if core_already_exists_response "${resp_file}"; then
      echo "Core '${core}' sudah ada (CREATE), lewati."
      continue
    fi
    echo "ERROR: CREATE core '${core}' gagal (HTTP atau wget). Respons:" >&2
    cat "${resp_file}" 2>/dev/null >&2 || true
    exit 1
  fi
  if grep -qE '"status"[[:space:]]*:[[:space:]]*0' "${resp_file}"; then
    echo "Core '${core}' selesai."
    continue
  fi
  if core_already_exists_response "${resp_file}"; then
    echo "Core '${core}' sudah ada (respons Solr), lewati."
    continue
  fi
  echo "ERROR: CREATE core '${core}' ditolak Solr:" >&2
  cat "${resp_file}" >&2
  exit 1
done

echo "Semua core Atlas/JanusGraph untuk Solr siap."
