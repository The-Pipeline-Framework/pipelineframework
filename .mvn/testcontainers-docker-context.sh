#!/bin/sh

# Export the active Docker CLI context in the form consumed by Testcontainers.
# Explicit Docker/Testcontainers environment variables always take precedence.
configure_testcontainers_docker_host() {
  [ -z "${DOCKER_HOST-}" ] || return 0
  command -v docker >/dev/null 2>&1 || return 0

  docker_context="$(docker context show 2>/dev/null)" || return 0
  [ -n "$docker_context" ] || return 0
  docker_host="$(docker context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}' 2>/dev/null)" || return 0
  case "$docker_host" in
  unix://* | tcp://*) ;;
  *) return 0 ;;
  esac

  if [ "${docker_host#tcp://}" != "$docker_host" ]; then
    docker_tls_material="$(docker context inspect "$docker_context" --format '{{json .TLSMaterial.docker}}' 2>/dev/null)" || return 1
    case "$docker_tls_material" in
    '' | null | '{}' | '<no value>') ;;
    *)
      if [ -z "${DOCKER_CERT_PATH-}" ]; then
        docker_tls_path="$(docker context inspect "$docker_context" --format '{{.Storage.TLSPath}}' 2>/dev/null)" || return 1
        docker_cert_path="${docker_tls_path%/}/docker"
        for certificate in ca.pem cert.pem key.pem; do
          if [ ! -r "$docker_cert_path/$certificate" ]; then
            printf '%s\n' "Docker context '$docker_context' uses TLS, but $docker_cert_path/$certificate is not readable" >&2
            return 1
          fi
        done
        DOCKER_CERT_PATH="$docker_cert_path"
        export DOCKER_CERT_PATH
      fi
      if [ -z "${DOCKER_TLS_VERIFY-}" ]; then
        DOCKER_TLS_VERIFY=1
        export DOCKER_TLS_VERIFY
      fi
      ;;
    esac
  fi

  DOCKER_HOST="$docker_host"
  export DOCKER_HOST

  if [ "$docker_context" = orbstack ] && [ -z "${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE-}" ]; then
    TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
    export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
  fi
}
