#!/bin/bash
# Generate test CA and server certificates for TLS E2E tests.
# Output: tests/certs/{ca.key, ca.crt, server.key, server.crt}
#
# The server certificate has SAN entries for DNS:localhost and IP:127.0.0.1,
# enabling both sslVerifyCa (IP) and sslVerifyFull (hostname) testing.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/certs"
mkdir -p "${CERT_DIR}"

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

# Clean up intermediates
rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/ca.srl"

echo "Certificates generated in ${CERT_DIR}:"
ls -la "${CERT_DIR}"
