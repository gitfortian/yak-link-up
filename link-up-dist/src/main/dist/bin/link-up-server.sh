#!/usr/bin/env bash
# Starts Link-Up Server in the foreground.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINK_UP_HOME="${LINK_UP_HOME:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
CONF_DIR="${LINK_UP_CONF_DIR:-${LINK_UP_HOME}/config}"
LOG_DIR="${LINK_UP_LOG_DIR:-${LINK_UP_HOME}/logs}"
LOGFILE="${LOGFILE:-${LOG_DIR}/link-up-server.log}"
JOB_LOG_DIR="${LINK_UP_JOB_LOG_DIR:-${LOG_DIR}/jobs}"
PLUGIN_ROOT="${LINK_UP_PLUGIN_ROOT:-${LINK_UP_HOME}/plugins}"
JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}java"

if ! command -v "${JAVA_BIN}" >/dev/null 2>&1; then
  echo "Java 8 or later is required; set JAVA_HOME or add java to PATH." >&2
  exit 1
fi

mkdir -p "$(dirname "${LOGFILE}")" "${JOB_LOG_DIR}"

JAVA_OPTS=(
  -Xms256m
  -Xmx1024m
  -XX:+ExitOnOutOfMemoryError
  -Dfile.encoding=UTF-8
  -Dlog4j.configurationFile="${CONF_DIR}/log4j2.xml"
  -Dlink.up.log.dir="${LOG_DIR}"
  -Dlink.up.log.file="${LOGFILE}"
  -Dlink.up.job.log.dir="${JOB_LOG_DIR}"
)

if [[ -n "${LINK_UP_JAVA_OPTS:-}" ]]; then
  # shellcheck disable=SC2206
  JAVA_OPTS+=( ${LINK_UP_JAVA_OPTS} )
fi

PLUGIN_ARGS=()

# Each plugin directory is intentionally passed separately. FactoryRegistry creates one
# ConnectorClassLoader per --plugin-dir, which is required for mutually incompatible SDK versions
# such as Elasticsearch 7 and Elasticsearch 8.
if [[ -n "${LINK_UP_PLUGIN_DIRS:-}" ]]; then
  IFS=',' read -r -a configured_plugin_dirs <<< "${LINK_UP_PLUGIN_DIRS}"
  for plugin_dir in "${configured_plugin_dirs[@]}"; do
    plugin_dir="${plugin_dir#${plugin_dir%%[![:space:]]*}}"
    plugin_dir="${plugin_dir%${plugin_dir##*[![:space:]]}}"
    if [[ -n "${plugin_dir}" ]]; then
      PLUGIN_ARGS+=( --plugin-dir "${plugin_dir}" )
    fi
  done
elif [[ -d "${PLUGIN_ROOT}" ]]; then
  shopt -s nullglob
  for plugin_dir in "${PLUGIN_ROOT}"/*; do
    if [[ -d "${plugin_dir}" ]]; then
      PLUGIN_ARGS+=( --plugin-dir "${plugin_dir}" )
    fi
  done
  shopt -u nullglob
fi

exec "${JAVA_BIN}" \
  "${JAVA_OPTS[@]}" \
  -cp "${LINK_UP_HOME}/lib/*" \
  com.link.up.server.FluxServer \
  "${PLUGIN_ARGS[@]}" \
  "$@"
