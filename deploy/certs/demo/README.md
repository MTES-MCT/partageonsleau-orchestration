# Redis CA for demo

The testing and production Redis certificates are tied to their respective
instances, so they must never be copied here.

Export the dedicated demo Redis CA as `redis-ca.pem` in this directory before
deploying, then configure
`REDIS_TLS_CA_FILE_PATH=/usr/local/share/ca-certificates/scw-redis-ca.crt` on
the demo container. The image build fails closed while the certificate is
missing.
