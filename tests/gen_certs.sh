#!/bin/bash
# Generate test CA and server certificates for TLS E2E tests.
# Output: tests/certs/ (CORE and DERIVED below)
#
# The server certificate has SAN entries for DNS:localhost and IP:127.0.0.1,
# enabling both sslVerifyCa (IP) and sslVerifyFull (hostname) testing.
# Existing files are reused unless REGENERATE=1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/certs"
mkdir -p "${CERT_DIR}"

# Rebuilding invalidates a running server's copy, so only when one is missing
# or REGENERATE=1.
CORE=(
  ca.crt
  server.crt
  server.key
  wrong_ca.crt
  wrong_ca.key
)

# Filled in individually without touching the core.
DERIVED=(
  encrypted.key
  ca.trusted.crt
  wrong_ca.rsa.key
  ec.key
)

# DERIVED fixtures built from CORE files, stale once the core is rebuilt.
CORE_DEPENDENT=(
  ca.trusted.crt
  wrong_ca.rsa.key
)

all_present() {
  local name
  for name in "$@"; do
    if [[ ! -f "${CERT_DIR}/${name}" ]]; then
      return 1
    fi
  done
  return 0
}

if [[ "${REGENERATE:-}" != "1" ]] && all_present "${CORE[@]}" "${DERIVED[@]}"; then
  echo "Using existing certificates in ${CERT_DIR}."
  exit 0
fi

if [[ "${REGENERATE:-}" == "1" ]] || ! all_present "${CORE[@]}"; then
  # --- CA ---
  openssl req -new -x509 -nodes \
    -days 3650 \
    -keyout "${CERT_DIR}/ca.key" \
    -out "${CERT_DIR}/ca.crt" \
    -subj "/CN=Test CA" \
    2>/dev/null

  # --- Server key + CSR ---
  openssl req -new -nodes \
    -keyout "${CERT_DIR}/server.key" \
    -out "${CERT_DIR}/server.csr" \
    -subj "/CN=localhost" \
    2>/dev/null

  # --- Sign server cert with CA (SAN: DNS:localhost, IP:127.0.0.1) ---
  openssl x509 -req \
    -in "${CERT_DIR}/server.csr" \
    -CA "${CERT_DIR}/ca.crt" \
    -CAkey "${CERT_DIR}/ca.key" \
    -CAcreateserial \
    -days 3650 \
    -out "${CERT_DIR}/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
    2>/dev/null

  # PostgreSQL requires server.key to be readable only by owner
  chmod 600 "${CERT_DIR}/server.key"

  # --- Wrong CA (for negative testing) ---
  openssl req -new -x509 -nodes \
    -days 3650 \
    -keyout "${CERT_DIR}/wrong_ca.key" \
    -out "${CERT_DIR}/wrong_ca.crt" \
    -subj "/CN=Wrong CA" \
    2>/dev/null

  chmod 600 "${CERT_DIR}/ca.key" "${CERT_DIR}/wrong_ca.key"

  # REGENERATE=1 rebuilds every fixture; otherwise only those tied to the core.
  if [[ "${REGENERATE:-}" == "1" ]]; then
    stale=("${DERIVED[@]}")
  else
    stale=("${CORE_DEPENDENT[@]}")
  fi
  for name in "${stale[@]}"; do
    rm -f "${CERT_DIR}/${name}"
  done
fi

# --- Passphrase-protected client key (password: test) ---
# Fixture for the "passphrase-protected key is rejected" TLS test. The
# password is a fixed test value; the key is never validated against a CA.
if [[ ! -f "${CERT_DIR}/encrypted.key" ]]; then
  openssl genrsa -aes256 -passout pass:test \
    -out "${CERT_DIR}/encrypted.key" 2048 \
    2>/dev/null
  chmod 600 "${CERT_DIR}/encrypted.key"
fi

# --- Legacy PEM banners (TRUSTED CERTIFICATE, PKCS#1, SEC1) ---
if [[ ! -f "${CERT_DIR}/ca.trusted.crt" ]]; then
  openssl x509 -in "${CERT_DIR}/ca.crt" -trustout -addtrust serverAuth \
    -out "${CERT_DIR}/ca.trusted.crt"
fi

if [[ ! -f "${CERT_DIR}/wrong_ca.rsa.key" ]]; then
  # -traditional is OpenSSL 3.x only; 1.1.1 and LibreSSL emit PKCS#1 by default.
  if ! openssl rsa -in "${CERT_DIR}/wrong_ca.key" -traditional \
    -out "${CERT_DIR}/wrong_ca.rsa.key" 2>/dev/null; then
    openssl rsa -in "${CERT_DIR}/wrong_ca.key" -out "${CERT_DIR}/wrong_ca.rsa.key" 2>/dev/null
  fi
  if ! grep -q "BEGIN RSA PRIVATE KEY" "${CERT_DIR}/wrong_ca.rsa.key"; then
    echo "error: openssl did not produce a PKCS#1 wrong_ca.rsa.key" >&2
    rm -f "${CERT_DIR}/wrong_ca.rsa.key"
    exit 1
  fi
  chmod 600 "${CERT_DIR}/wrong_ca.rsa.key"
fi

if [[ ! -f "${CERT_DIR}/ec.key" ]]; then
  openssl ecparam -name prime256v1 -genkey -noout \
    -out "${CERT_DIR}/ec.key"
  chmod 600 "${CERT_DIR}/ec.key"
fi

# Clean up intermediates
rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/ca.srl"

echo "Certificates generated in ${CERT_DIR}:"
ls -la "${CERT_DIR}"
