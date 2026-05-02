#!/usr/bin/env bash
# =============================================================================
# Operation FRAUD BUSTER — One-Click Setup Script
# =============================================================================
# Spins up OLTP + OLAP PostgreSQL, builds Airflow, deploys ETL DAG.
# OLTP uses a pre-built Docker image with data already loaded.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh          # full setup
#   ./setup.sh --clean                        # tear down everything
#   ./setup.sh --status                       # check current state
#
# Platforms: macOS, Linux, Windows (WSL2 / Git Bash)
# Requirements: Docker, Docker Compose v2+, bash
# =============================================================================

set -euo pipefail

# ── Colors (safe, disabled if not a terminal) ────────────────────────────────
if [ -t 1 ]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
    CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; CYAN=''; BOLD=''; NC=''
fi

# ── Helpers ──────────────────────────────────────────────────────────────────
info()    { printf "${CYAN}[INFO]${NC}  %s\n" "$*"; }
ok()      { printf "${GREEN}[OK]${NC}    %s\n" "$*"; }
warn()    { printf "${YELLOW}[WARN]${NC}  %s\n" "$*"; }
fail()    { printf "${RED}[FAIL]${NC}  %s\n" "$*"; exit 1; }
step()    { printf "\n${BOLD}━━━ Step %s ━━━${NC}\n" "$*"; }

# ── Resolve script directory (cross-platform) ───────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_DIR="$SCRIPT_DIR/airflow"
DATASET_DIR="$SCRIPT_DIR/dataset"
DAGS_DIR="$AIRFLOW_DIR/dags"
DATA_DIR="$AIRFLOW_DIR/data"

# ── Configuration ────────────────────────────────────────────────────────────
OLTP_CONTAINER="fraud-oltp-db"
OLAP_CONTAINER="fraud-olap-db"
OLTP_PORT=25432
OLAP_PORT=25433
AIRFLOW_PORT=28080
AIRFLOW_PG_PORT=25434

OLTP_USER="ndc_user"
OLTP_PASS="ndc_password_2025"
OLTP_DB="ndc_oltp"

OLAP_USER="ndc_analyst"
OLAP_PASS="ndc_analyst_2025"
OLAP_DB="ndc_olap"

# Pre-built OLTP image — update this after pushing to Docker Hub
# To build: ./build-image.sh
# To push:  ./build-image.sh --push <dockerhub-username>
OLTP_IMAGE="hedypamungkas/fraud-oltp-db:loaded"

# Detect Docker Compose command (v2 plugin vs standalone)
if docker compose version &>/dev/null; then
    DC="docker compose"
elif command -v docker-compose &>/dev/null; then
    DC="docker-compose"
else
    fail "Docker Compose not found. Install it first: https://docs.docker.com/compose/install/"
fi

# Detect host.docker.internal availability (Linux needs extra flag)
DOCKER_HOST_GATEWAY=""
if [[ "$(uname -s)" == "Linux" ]] && ! docker run --rm alpine ping -c1 host.docker.internal &>/dev/null 2>&1; then
    DOCKER_HOST_GATEWAY="--add-host=host.docker.internal:host-gateway"
fi

# =============================================================================
# COMMANDS
# =============================================================================

cmd_clean() {
    step "CLEANUP — Removing all containers and volumes"
    info "Stopping Airflow..."
    (cd "$AIRFLOW_DIR" && $DC down -v --remove-orphans 2>/dev/null) || true
    info "Removing database containers..."
    docker rm -f "$OLTP_CONTAINER" "$OLAP_CONTAINER" 2>/dev/null || true
    ok "All containers removed"
}

cmd_status() {
    step "STATUS CHECK"
    local all_ok=true

    # Docker
    if command -v docker &>/dev/null && docker info &>/dev/null; then
        ok "Docker is running"
    else
        fail "Docker is not running"; all_ok=false
    fi

    # OLTP
    if docker ps --format '{{.Names}}' | grep -q "^${OLTP_CONTAINER}$"; then
        local cnt
        cnt=$(docker exec "$OLTP_CONTAINER" psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "SELECT COUNT(*) FROM ndc_customers" 2>/dev/null || echo "0")
        if [ "$cnt" -gt 0 ]; then ok "OLTP ($OLTP_CONTAINER): $cnt customers"; else warn "OLTP running but no data"; all_ok=false; fi
    else
        warn "OLTP not running"; all_ok=false
    fi

    # OLAP
    if docker ps --format '{{.Names}}' | grep -q "^${OLAP_CONTAINER}$"; then
        local tables
        tables=$(docker exec "$OLAP_CONTAINER" psql -U "$OLAP_USER" -d "$OLAP_DB" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null || echo "0")
        if [ "$tables" -gt 0 ]; then ok "OLAP ($OLAP_CONTAINER): $tables tables"; else warn "OLAP running but no tables"; all_ok=false
        fi
    else
        warn "OLAP not running"; all_ok=false
    fi

    # Airflow
    if curl -sf "http://localhost:${AIRFLOW_PORT}/health" | grep -q "healthy\|running" 2>/dev/null; then
        ok "Airflow UI: http://localhost:${AIRFLOW_PORT}"
    else
        warn "Airflow not reachable"; all_ok=false
    fi

    if $all_ok; then ok "Everything is ready!"; else warn "Some components are missing — run ./setup.sh"; fi
}

# =============================================================================
# SETUP STEPS
# =============================================================================

step_1_prerequisites() {
    step "1/7 — Checking prerequisites"

    command -v docker &>/dev/null || fail "Docker not found. Install: https://docs.docker.com/get-docker/"
    docker info &>/dev/null       || fail "Docker daemon not running. Start Docker Desktop or dockerd."

    local docker_ver
    docker_ver=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "unknown")
    ok "Docker $docker_ver"

    # Check ports
    for port in $OLTP_PORT $OLAP_PORT $AIRFLOW_PORT $AIRFLOW_PG_PORT; do
        if lsof -i ":$port" &>/dev/null 2>&1 || ss -tlnp 2>/dev/null | grep -q ":$port " || netstat -an 2>/dev/null | grep -q "\.$port "; then
            if docker ps --format '{{.Names}}' | grep -qE "fraud-|airflow"; then
                warn "Port $port in use by existing container (will be reused or replaced)"
            else
                warn "Port $port is in use by another process — may cause conflicts"
            fi
        fi
    done

    # Verify OLTP image exists
    if docker image inspect "$OLTP_IMAGE" &>/dev/null; then
        ok "OLTP image found: $OLTP_IMAGE (local)"
    else
        info "OLTP image not found locally, pulling $OLTP_IMAGE..."
        docker pull "$OLTP_IMAGE" 2>/dev/null || fail "Cannot pull OLTP image. Run ./build-image.sh first, or update OLTP_IMAGE variable."
        ok "OLTP image pulled: $OLTP_IMAGE"
    fi

    # Verify other files exist
    for f in "$DATASET_DIR/ndc-olap-schema.sql" \
             "$DATASET_DIR/customer_complaints.csv" "$DATASET_DIR/ip_device_log.csv" "$DATASET_DIR/promo_codes.csv" \
             "$AIRFLOW_DIR/docker-compose.yml" "$DAGS_DIR/dag_fraud_etl.py"; do
        [ -f "$f" ] || fail "Required file missing: $f"
    done
    ok "All required files found"
}

step_2_start_oltp() {
    step "2/7 — Starting OLTP PostgreSQL (port $OLTP_PORT) [pre-loaded]"

    docker rm -f "$OLTP_CONTAINER" &>/dev/null || true

    docker run -d \
        --name "$OLTP_CONTAINER" \
        --shm-size=512m \
        -p "${OLTP_PORT}:5432" \
        $DOCKER_HOST_GATEWAY \
        "$OLTP_IMAGE" >/dev/null

    _wait_pg "$OLTP_CONTAINER" "$OLTP_USER" "$OLTP_DB" 60

    # Verify data with retries — PostgreSQL may need crash recovery
    local cnt retry=0 max_retries=15
    while [ $retry -lt $max_retries ]; do
        cnt=$(docker exec "$OLTP_CONTAINER" psql -U "$OLTP_USER" -d "$OLTP_DB" -tAc "SELECT COUNT(*) FROM ndc_customers" 2>/dev/null || echo "0")
        if [ "$cnt" -gt 0 ]; then
            ok "OLTP started with $cnt customers (data pre-loaded)"
            break
        fi
        retry=$((retry + 1))
        info "  Waiting for data... (attempt $retry/$max_retries)"
        sleep 2
    done
    if [ "$cnt" -eq 0 ] || [ -z "$cnt" ]; then
        # Show container logs for debugging
        warn "Container logs (last 20 lines):"
        docker logs "$OLTP_CONTAINER" 2>&1 | tail -20
        fail "OLTP started but no data found — image may be corrupt"
    fi
}

step_3_start_olap() {
    step "3/7 — Starting OLAP PostgreSQL (port $OLAP_PORT)"

    docker rm -f "$OLAP_CONTAINER" &>/dev/null || true

    docker run -d \
        --name "$OLAP_CONTAINER" \
        -e POSTGRES_USER="$OLAP_USER" \
        -e POSTGRES_PASSWORD="$OLAP_PASS" \
        -e POSTGRES_DB="$OLAP_DB" \
        -p "${OLAP_PORT}:5432" \
        $DOCKER_HOST_GATEWAY \
        postgres:15 >/dev/null

    _wait_pg "$OLAP_CONTAINER" "$OLAP_USER" "$OLAP_DB" 60
    ok "OLAP PostgreSQL started"
}

step_4_load_olap() {
    step "4/7 — Loading OLAP star schema"

    docker cp "$DATASET_DIR/ndc-olap-schema.sql" "$OLAP_CONTAINER":/tmp/olap-schema.sql
    docker exec "$OLAP_CONTAINER" psql -U "$OLAP_USER" -d "$OLAP_DB" -f /tmp/olap-schema.sql >/dev/null 2>&1

    local table_count
    table_count=$(docker exec "$OLAP_CONTAINER" psql -U "$OLAP_USER" -d "$OLAP_DB" -tAc \
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null)
    ok "Star schema loaded ($table_count tables)"
}

step_5_prepare_airflow() {
    step "5/7 — Preparing Airflow data & config"

    mkdir -p "$DATA_DIR" "$AIRFLOW_DIR/logs" "$AIRFLOW_DIR/plugins"

    # Copy CSV files
    for csv in customer_complaints.csv ip_device_log.csv promo_codes.csv; do
        cp "$DATASET_DIR/$csv" "$DATA_DIR/"
    done
    ok "CSV files copied to $DATA_DIR"

    # Handle duplicate DAG
    if [ -f "$DAGS_DIR/dag_fraud_etl_student.py" ] && [ -f "$DAGS_DIR/dag_fraud_etl.py" ]; then
        mv "$DAGS_DIR/dag_fraud_etl_student.py" "$DAGS_DIR/dag_fraud_etl_student.py.bak"
        info "Student DAG renamed to .bak (full solution active)"
    fi

    # Build custom Airflow image if Dockerfile exists
    if [ -f "$AIRFLOW_DIR/Dockerfile" ]; then
        info "Building custom Airflow image..."
        (cd "$AIRFLOW_DIR" && docker build -t fraud-buster-airflow . >/dev/null 2>&1)
        ok "Custom Airflow image built"
    fi
}

step_6_start_airflow() {
    step "6/7 — Starting Airflow services"

    cd "$AIRFLOW_DIR"
    $DC up -d --build 2>&1 | tail -1
    cd "$SCRIPT_DIR"

    # Wait for Airflow webserver
    info "Waiting for Airflow webserver (up to 2 min)..."
    local waited=0
    while [ $waited -lt 120 ]; do
        if curl -sf "http://localhost:${AIRFLOW_PORT}/health" | grep -q "healthy\|running" 2>/dev/null; then
            ok "Airflow webserver ready"
            break
        fi
        sleep 5
        waited=$((waited + 5))
    done
    if [ $waited -ge 120 ]; then
        fail "Airflow webserver did not start within 2 minutes"
    fi

    # Create connections
    info "Creating database connections..."
    docker exec "$($DC -f "$AIRFLOW_DIR/docker-compose.yml" ps -q airflow-scheduler 2>/dev/null | head -1)" \
        airflow connections add ndc_oltp \
        --conn-type postgres \
        --conn-host host.docker.internal \
        --conn-port "$OLTP_PORT" \
        --conn-login "$OLTP_USER" \
        --conn-password "$OLTP_PASS" \
        --conn-schema "$OLTP_DB" 2>/dev/null || true

    docker exec "$($DC -f "$AIRFLOW_DIR/docker-compose.yml" ps -q airflow-scheduler 2>/dev/null | head -1)" \
        airflow connections add ndc_olap \
        --conn-type postgres \
        --conn-host host.docker.internal \
        --conn-port "$OLAP_PORT" \
        --conn-login "$OLAP_USER" \
        --conn-password "$OLAP_PASS" \
        --conn-schema "$OLAP_DB" 2>/dev/null || true

    ok "Connections created (ndc_oltp, ndc_olap)"
}

step_7_verify() {
    step "7/7 — Final verification"

    local scheduler_container
    scheduler_container=$($DC -f "$AIRFLOW_DIR/docker-compose.yml" ps -q airflow-scheduler 2>/dev/null | head -1)

    # Check DAG is loaded
    sleep 10
    local dag_loaded
    dag_loaded=$(docker exec "$scheduler_container" airflow dags list 2>/dev/null | grep "fraud_buster_etl" | head -1 || true)
    if [ -n "$dag_loaded" ]; then
        ok "DAG 'fraud_buster_etl' loaded successfully"
    else
        local dag_err
        dag_err=$(docker exec "$scheduler_container" airflow dags list-import-errors 2>/dev/null | head -3 || true)
        if [ -n "$dag_err" ]; then
            warn "DAG import error: $dag_err"
        fi
    fi

    echo ""
    printf "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════╗${NC}\n"
    printf "${GREEN}${BOLD}║          OPERATION FRAUD BUSTER — READY!                 ║${NC}\n"
    printf "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════╣${NC}\n"
    printf "${GREEN}${BOLD}║${NC}                                                          ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}  Airflow UI  : ${CYAN}http://localhost:${AIRFLOW_PORT}${NC}  (admin/admin)    ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}  OLTP DB     : ${CYAN}localhost:${OLTP_PORT}${NC}  (${OLTP_USER})          ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}  OLAP DB     : ${CYAN}localhost:${OLAP_PORT}${NC}  (${OLAP_USER})          ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}                                                          ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}  ${YELLOW}Next:${NC} Trigger DAG 'fraud_buster_etl' in Airflow UI      ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}║${NC}                                                          ${GREEN}${BOLD}║${NC}\n"
    printf "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════╝${NC}\n"
}

# =============================================================================
# INTERNAL HELPERS
# =============================================================================

_wait_pg() {
    local container="$1" user="$2" db="$3" max_wait="${4:-60}"
    info "Waiting for $container to accept connections..."
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if docker exec "$container" pg_isready -U "$user" -d "$db" &>/dev/null; then
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    fail "Database $container did not become ready within ${max_wait}s"
}

# =============================================================================
# MAIN
# =============================================================================

case "${1:-}" in
    --clean)
        cmd_clean
        ;;
    --status)
        cmd_status
        ;;
    --help|-h)
        echo "Usage: $0 [--clean|--status|--help]"
        echo ""
        echo "  (no flag)  Full setup: start DBs, load data, start Airflow"
        echo "  --clean    Stop and remove all containers & volumes"
        echo "  --status   Check health of all components"
        echo "  --help     Show this help"
        ;;
    *)
        echo ""
        printf "${BOLD}  ╦ ╦╔═╗╔╗ ╔═╗╦ ╦╔═╗╦  ╦  ${NC}\n"
        printf "${BOLD}  ║║║║╣ ╠╩╗╚═╗╠═╣║╣ ║  ║  ${NC}\n"
        printf "${BOLD}  ╚╩╝╚═╝╚═╝╚═╝╩ ╩╚═╝╩═╝╩═╝${NC}\n"
        printf "${BOLD}  FRAUD BUSTER — Setup${NC}\n\n"

        step_1_prerequisites
        step_2_start_oltp
        step_3_start_olap
        step_4_load_olap
        step_5_prepare_airflow
        step_6_start_airflow
        step_7_verify
        ;;
esac
