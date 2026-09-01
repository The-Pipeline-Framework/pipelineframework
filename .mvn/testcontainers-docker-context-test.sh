#!/bin/sh

set -eu

script_dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
. "$script_dir/testcontainers-docker-context.sh"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

assert_equal() {
  [ "$1" = "$2" ] || fail "expected '$2', got '$1'"
}

test_orbstack_context() (
  unset DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
  docker() {
    case "$*" in
    'context show') printf '%s\n' orbstack ;;
    *'{{.Endpoints.docker.Host}}'*) printf '%s\n' 'unix:///Users/test/.orbstack/run/docker.sock' ;;
    *) return 1 ;;
    esac
  }

  configure_testcontainers_docker_host
  assert_equal "$DOCKER_HOST" 'unix:///Users/test/.orbstack/run/docker.sock'
  assert_equal "$TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE" '/var/run/docker.sock'
)

test_tls_tcp_context() (
  unset DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
  tls_root="$(mktemp -d)"
  trap 'rm -rf "$tls_root"' EXIT HUP INT TERM
  mkdir -p "$tls_root/docker"
  : >"$tls_root/docker/ca.pem"
  : >"$tls_root/docker/cert.pem"
  : >"$tls_root/docker/key.pem"

  docker() {
    case "$*" in
    'context show') printf '%s\n' remote-tls ;;
    *'{{.Endpoints.docker.Host}}'*) printf '%s\n' 'tcp://docker.example.test:2376' ;;
    *'{{json .TLSMaterial.docker}}'*) printf '%s\n' '{"ca.pem":"ca.pem","cert.pem":"cert.pem","key.pem":"key.pem"}' ;;
    *'{{.Storage.TLSPath}}'*) printf '%s\n' "$tls_root" ;;
    *) return 1 ;;
    esac
  }

  configure_testcontainers_docker_host
  assert_equal "$DOCKER_HOST" 'tcp://docker.example.test:2376'
  assert_equal "$DOCKER_TLS_VERIFY" '1'
  assert_equal "$DOCKER_CERT_PATH" "$tls_root/docker"
  [ -r "$DOCKER_CERT_PATH/ca.pem" ] || fail 'missing propagated CA certificate'
  [ -r "$DOCKER_CERT_PATH/cert.pem" ] || fail 'missing propagated client certificate'
  [ -r "$DOCKER_CERT_PATH/key.pem" ] || fail 'missing propagated client key'
)

test_orbstack_context
test_tls_tcp_context
