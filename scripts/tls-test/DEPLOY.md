# pNFS-MDS 5-VM TLS Integration Test — Deployment Runbook

## Cluster Layout

| VM | Hostname | IP | Roles |
|---|---|---|---|
| 1 | mds1 | 10.10.10.50 | MDS node 1 (active) + etcd member 1 |
| 2 | mds2 | 10.10.10.51 | MDS node 2 (active) + etcd member 2 |
| 3 | ds1 | 10.10.10.52 | Data server 1 (NFS export) |
| 4 | ds2 | 10.10.10.53 | Data server 2 (NFS export) |
| 5 | client1 | 10.10.10.54 | NFS client + mds-admin + etcd member 3 |

All VMs: Ubuntu 24.04, user `peak`, passwordless sudo.

---

## Prerequisites

### 1. SSH Key Distribution

Generate a key on the control host and copy to all VMs:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/cluster_test -N ""

export SSHPASS="<password>"
for ip in 10.10.10.{50..54}; do
    sshpass -e ssh-copy-id -i ~/.ssh/cluster_test.pub \
        -o StrictHostKeyChecking=no "peak@${ip}"
done
```

All subsequent commands use:
```bash
SSH="ssh -i ~/.ssh/cluster_test -o StrictHostKeyChecking=no"
SCP="scp -i ~/.ssh/cluster_test -o StrictHostKeyChecking=no"
```

### 2. Common Packages (all 5 VMs)

```bash
for ip in 10.10.10.{50..54}; do
    $SSH "peak@${ip}" "sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq && \
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        build-essential cmake liblmdb-dev libssl-dev libkrb5-dev \
        nfs-common tcpdump"
done
```

---

## Step 1: TLS Certificates

Generate on the control host, then distribute.

### 1a. Create CA + Per-Node Certs

```bash
CERTDIR="scripts/tls-test/certs"
mkdir -p "${CERTDIR}"

# CA (30-day validity)
openssl req -x509 -newkey rsa:2048 \
    -keyout "${CERTDIR}/ca.key" -out "${CERTDIR}/ca.crt" \
    -days 30 -nodes -subj "/CN=pnfs-mds-test-ca/O=PeakAIO-Test"

# Per-node certs with SAN
declare -A NODE_IPS=(
    [mds1]=10.10.10.50 [mds2]=10.10.10.51
    [ds1]=10.10.10.52  [ds2]=10.10.10.53
    [client]=10.10.10.54
)

for node in "${!NODE_IPS[@]}"; do
    ip="${NODE_IPS[$node]}"
    cat > /tmp/san.cnf <<EOF
[req]
default_bits = 2048
prompt = no
distinguished_name = dn
req_extensions = v3_req
[dn]
CN = ${node}
O = PeakAIO-Test
[v3_req]
subjectAltName = DNS:${node},DNS:${node}.local,IP:${ip},IP:127.0.0.1
[v3_ext]
subjectAltName = DNS:${node},DNS:${node}.local,IP:${ip},IP:127.0.0.1
EOF
    openssl req -newkey rsa:2048 -nodes \
        -keyout "${CERTDIR}/${node}.key" -out /tmp/${node}.csr \
        -config /tmp/san.cnf
    openssl x509 -req -in /tmp/${node}.csr \
        -CA "${CERTDIR}/ca.crt" -CAkey "${CERTDIR}/ca.key" \
        -CAcreateserial -out "${CERTDIR}/${node}.crt" \
        -days 30 -extfile /tmp/san.cnf -extensions v3_ext
    rm -f /tmp/${node}.csr /tmp/san.cnf
done
rm -f "${CERTDIR}/ca.srl"
```

### 1b. Distribute Certs

Each node gets `ca.crt` + its own `<name>.crt` + `<name>.key`:

```bash
for node in mds1 mds2 ds1 ds2 client; do
    ip="${NODE_IPS[$node]}"
    $SSH "peak@${ip}" "mkdir -p ~/certs"
    $SCP "${CERTDIR}/ca.crt" "${CERTDIR}/${node}.crt" \
         "${CERTDIR}/${node}.key" "peak@${ip}:~/certs/"
done
```

---

## Step 2: etcd Cluster (VMs 1, 2, 5)

### 2a. Install

```bash
for ip in 10.10.10.50 10.10.10.51 10.10.10.54; do
    $SSH "peak@${ip}" "sudo apt-get install -y -qq etcd-server etcd-client"
done
```

### 2b. Configure

**Known issue:** On Ubuntu 24.04, etcd runs as the `etcd` system user
via systemd. Cert files must be readable by this user. Copy certs to
`/etc/etcd/` and set ownership:

```bash
for ip in 10.10.10.50 10.10.10.51 10.10.10.54; do
    $SSH "peak@${ip}" "
        sudo mkdir -p /etc/etcd
        sudo cp ~/certs/* /etc/etcd/
        sudo chown -R etcd:etcd /etc/etcd/
        sudo chmod 644 /etc/etcd/*.crt
        sudo chmod 640 /etc/etcd/*.key
    "
done
```

Write `/etc/default/etcd` on each node. The template below uses
`${NAME}`, `${IP}`, `${CERT}` — substitute per node.

```bash
CLUSTER="mds1=https://10.10.10.50:2380,mds2=https://10.10.10.51:2380,etcd3=https://10.10.10.54:2380"
TOKEN="pnfs-mds-test-cluster"

# Node-specific values:
#   mds1:   NAME=mds1  IP=10.10.10.50 CERT=mds1
#   mds2:   NAME=mds2  IP=10.10.10.51 CERT=mds2
#   client: NAME=etcd3 IP=10.10.10.54 CERT=client

cat <<EOF | sudo tee /etc/default/etcd
ETCD_NAME="${NAME}"
ETCD_DATA_DIR="/var/lib/etcd/default"
ETCD_LISTEN_PEER_URLS="https://${IP}:2380"
ETCD_LISTEN_CLIENT_URLS="https://${IP}:2379,https://127.0.0.1:2379"
ETCD_INITIAL_ADVERTISE_PEER_URLS="https://${IP}:2380"
ETCD_ADVERTISE_CLIENT_URLS="https://${IP}:2379"
ETCD_INITIAL_CLUSTER="${CLUSTER}"
ETCD_INITIAL_CLUSTER_TOKEN="${TOKEN}"
ETCD_INITIAL_CLUSTER_STATE="new"
ETCD_CERT_FILE="/etc/etcd/${CERT}.crt"
ETCD_KEY_FILE="/etc/etcd/${CERT}.key"
ETCD_TRUSTED_CA_FILE="/etc/etcd/ca.crt"
ETCD_CLIENT_CERT_AUTH="true"
ETCD_PEER_CERT_FILE="/etc/etcd/${CERT}.crt"
ETCD_PEER_KEY_FILE="/etc/etcd/${CERT}.key"
ETCD_PEER_TRUSTED_CA_FILE="/etc/etcd/ca.crt"
ETCD_PEER_CLIENT_CERT_AUTH="true"
EOF
```

**Known issue:** If etcd was previously started with default config,
stale data in `/var/lib/etcd/default` will cause cluster ID mismatch.
Clear it before first TLS start:

```bash
sudo rm -rf /var/lib/etcd/default
```

### 2c. Start + Verify

Start all three nodes (order doesn't matter — they retry peer connections):

```bash
for ip in 10.10.10.50 10.10.10.51 10.10.10.54; do
    $SSH "peak@${ip}" "sudo systemctl restart etcd"
done
sleep 5
```

Verify cluster health (from any member):

```bash
ETCDCTL_API=3 etcdctl \
    --endpoints=https://10.10.10.50:2379 \
    --cacert=/etc/etcd/ca.crt \
    --cert=/etc/etcd/mds1.crt \
    --key=/etc/etcd/mds1.key \
    endpoint health --cluster
```

Expected: all three endpoints report `is healthy: true`.

---

## Step 3: Data Servers (VMs 3, 4)

### 3a. Create Export Directories + NFS Server

```bash
for ip in 10.10.10.52 10.10.10.53; do
    $SSH "peak@${ip}" "
        sudo apt-get install -y -qq nfs-kernel-server
        sudo mkdir -p /export/ds
        sudo chown nobody:nogroup /export/ds
        echo '/export/ds 10.10.10.0/24(rw,sync,no_subtree_check,no_root_squash)' | \
            sudo tee /etc/exports
        sudo exportfs -ra
        sudo systemctl restart nfs-kernel-server
    "
done
```

Verify from an MDS node:
```bash
showmount -e 10.10.10.52
showmount -e 10.10.10.53
```

---

## Step 4: Build + Deploy pnfs-mds (VMs 1, 2)

### 4a. Copy Source + Build

```bash
for ip in 10.10.10.50 10.10.10.51; do
    rsync -az -e "ssh -i ~/.ssh/cluster_test" \
        --exclude=build --exclude=.git \
        /home/peak/code/pnfs-mds/ "peak@${ip}:~/pnfs-mds/"

    $SSH "peak@${ip}" "
        mkdir -p ~/pnfs-mds/build
        cd ~/pnfs-mds/build
        cmake .. -DCMAKE_BUILD_TYPE=Release
        cmake --build . -j\$(nproc)
    "
done
```

### 4b. Create MDS Config

Template for MDS 1 (`/home/peak/pnfs-mds/mds1.conf`):

```ini
mds_id = 1
hostname = mds1
nfs_port = 2049
grpc_port = 9800
cluster_size = 2
worker_threads = 4
lmdb_path = /home/peak/mds-data/metadata.lmdb
lmdb_map_size = 1073741824

# Cluster transport TLS
cluster_ca_file = /home/peak/certs/ca.crt
node_cert_file = /home/peak/certs/mds1.crt
node_key_file = /home/peak/certs/mds1.key
require_mtls = true

# etcd (TLS)
etcd_endpoints = https://10.10.10.50:2379,https://10.10.10.51:2379,https://10.10.10.54:2379
etcd_ca_file = /home/peak/certs/ca.crt
etcd_cert_file = /home/peak/certs/mds1.crt
etcd_key_file = /home/peak/certs/mds1.key

# Data servers
ds_count = 2
ds[0] = 10.10.10.52:/export/ds
ds[1] = 10.10.10.53:/export/ds
ds_mount_path_fmt = /mnt/ds%u

# Replication (MDS1 -> MDS2)
standby_host = 10.10.10.51
standby_port = 9810
```

MDS 2 config is identical except:
- `mds_id = 2`, `hostname = mds2`
- Cert paths use `mds2.crt`/`mds2.key`
- Replication standby points to MDS 1 (or omitted if active-active)

### 4c. Create Data Directory + Mount DS Exports

```bash
$SSH "peak@10.10.10.50" "
    mkdir -p ~/mds-data
    sudo mkdir -p /mnt/ds0 /mnt/ds1
    sudo mount -t nfs 10.10.10.52:/export/ds /mnt/ds0
    sudo mount -t nfs 10.10.10.53:/export/ds /mnt/ds1
"
```

### 4d. Start MDS

```bash
$SSH "peak@10.10.10.50" "
    cd ~/pnfs-mds
    ./build/pnfs-mds mds1.conf &
"
```

---

## Step 5: Client Testing (VM 5)

### 5a. Mount NFS

```bash
sudo mount -t nfs4.1 10.10.10.50:/ /mnt/test
```

### 5b. Verify TLS Traffic

On an MDS node, capture cluster transport traffic:
```bash
sudo tcpdump -i any port 9800 -c 20 -w /tmp/cluster.pcap
```

Then from the client, trigger a cross-MDS operation:
```bash
./build/mds-admin --host 10.10.10.50 --port 9800 status
```

In the pcap, traffic on port 9800 should be TLS (starts with
`0x16 0x03` — TLS ClientHello), not plaintext CT_MSG headers.

### 5c. Negative Tests

**Wrong cert:**
```bash
# Copy ds1's cert to the MDS cert path — should fail TLS handshake
```

**No TLS, remote access:**
```bash
# With cluster_ca_file empty, admin requests from 10.10.10.54
# should be rejected (loopback-only guard, finding 3.4)
```

---

## Troubleshooting

### etcd won't start: "permission denied" on cert files
etcd runs as the `etcd` user on Ubuntu. Certs must be in a directory
readable by that user (e.g. `/etc/etcd/`) with key files `chmod 640`
and `chown etcd:etcd`.

### etcd won't start: "cluster ID mismatch"
Stale data from a previous run. Clear with:
```bash
sudo rm -rf /var/lib/etcd/default
sudo systemctl restart etcd
```

### etcd "peer connection error: dial tcp: connect: connection refused"
Normal during startup — peers retry. Wait 5-10 seconds for all three
nodes to reach quorum.

### MDS can't connect to etcd: "tls: bad certificate"
Check that the MDS config's `etcd_cert_file` points to a cert signed
by the same CA that etcd's `ETCD_TRUSTED_CA_FILE` references. Verify
with:
```bash
openssl verify -CAfile /etc/etcd/ca.crt ~/certs/mds1.crt
```

### Admin CLI rejected: "connection refused"
Expected when no TLS and no peer allowlist are configured (finding 3.4
loopback guard). Either configure TLS or add the client IP to
`cluster_allowed_peers` in the MDS config.

### Build fails on Ubuntu 24.04 (GCC 14): "-Werror=maybe-uninitialized"
GCC 14 is stricter than GCC 13. The ntirpc system header
`/usr/include/ntirpc/rpc/xdr_inline.h` triggers false-positive
maybe-uninitialized warnings, and several test files trigger
unused-variable warnings. Add suppressions to CMakeLists.txt:

```
-Wno-error=maybe-uninitialized -Wno-error=unused-result
-Wno-error=unused-but-set-variable -Wno-error=unused-variable
```

Or edit the `-Werror` line in CMakeLists.txt directly:
```bash
sed -i 's/-Werror/-Werror -Wno-error=maybe-uninitialized -Wno-error=unused-result -Wno-error=unused-but-set-variable -Wno-error=unused-variable/' CMakeLists.txt
```

The main `pnfs-mds` and `mds-admin` binaries build successfully.
Some test targets may still fail — those are not needed for the
integration test.

### MDS fails with "self-registration wait_for_revision timed out"
The etcd HTTP client may fail to parse the etcd v3 response over TLS
even though the TLS handshake succeeds. Verified with curl that the
etcd v3 HTTP API works correctly with the same certs. Likely a
response parsing issue in `etcd_client.c:http_post()` — the TLS
read path may return data differently than the raw socket path.

**Status:** Under investigation. The `https://` endpoint parsing fix
(commit 853c704) resolved the initial connection rejection. The
remaining issue is in the HTTP response read loop over TLS.

### Config file deleted by rsync --delete
When syncing source code with `rsync --delete`, manually created
config files in the source tree are removed. Place config files
outside the source tree (e.g. `~/mds1.conf`) or exclude them:
```bash
rsync --exclude='*.conf' ...
```

---

## Integration Test Results (March 26, 2026)

### Bugs Found and Fixed

**TLS transport (5 bugs):**
1. `https://` endpoint rejection — `parse_endpoint` only accepted `http://`
2. Watch stream plaintext — `watch_connect_and_create` bypassed TLS
3. Interior-pointer free in threadpool `rpc_work_fn` (heap corruption)
4. Keepalive first-sleep delay — thread slept before first lease refresh
5. ALPN `http/1.1` broke etcd — removed (etcd defaults to HTTP/1.1)

**IP address verification:**
6. `SSL_set1_host` doesn't match IP SANs — use `X509_VERIFY_PARAM_set1_ip_asc`

**NFS mount (2 bugs):**
7. `NFS4_MINOR_VERSION_MIN = 1` rejected kernel v4.0 probe — changed to 0
8. SEQUENCE enforcement for v4.0 compounds — skip for `minorversion == 0`

### Known Issues (deferred)

**etcd TLS keepalive race:**
Concurrent TLS handshakes from the same `SSL_CTX` to etcd 3.4 on
Ubuntu 24.04 / OpenSSL 3.0 cause intermittent connection failures.
Plaintext etcd works perfectly. Root cause: likely OpenSSL session
cache or internal state corruption under concurrent `SSL_new/SSL_connect`.
Workaround: use plaintext etcd for internal cluster coordination.

**NFS mount — root inode type:**
After fixing minorversion and SEQUENCE enforcement, the v4.0 probe
COMPOUND (PUTROOTFH + GETATTR) succeeds at the RPC level but returns
`type=NON` (0) instead of `NF4DIR` (2) for the root directory. The
kernel NFS client rejects this and returns EIO. Root cause: fresh LMDB
database does not bootstrap a root inode with the correct directory type.
Next step: ensure `mds_lmdb_open` creates root inode (fileid 2) with
`MDS_FTYPE_DIR` if it doesn't exist.

**GCC 14 build warnings:**
Ubuntu 24.04's GCC 14 is stricter than GCC 13. The ntirpc system header
and several test files trigger `-Werror` failures. Suppressions needed:
```
-Wno-error=maybe-uninitialized -Wno-error=unused-result
-Wno-error=unused-but-set-variable -Wno-error=unused-variable
```

### Cluster Status (working)

- 3-member etcd cluster: healthy (plaintext HTTP)
- MDS 1 + MDS 2: running, zero CRIT, registered in etcd
- DS 1 + DS 2: NFS exports up, mounted on both MDS nodes
- Cluster transport port 9800: listening with TLS
- NFS port 2049: listening, processes RPC NULL + v4.0/v4.1 COMPOUNDs
