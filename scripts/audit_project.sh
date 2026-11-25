#!/usr/bin/env bash

set +e

REPORT="PROJECT_AUDIT_REPORT.txt"

echo "Starting Egypt Air Quality Project Audit..."

exec > >(tee "$REPORT") 2>&1

section() {
    echo ""
    echo "=================================================="
    echo "$1"
    echo "=================================================="
}

section "1. SYSTEM INFORMATION"

date
uname -a
lsb_release -a 2>/dev/null
docker --version
docker compose version
python3 --version
git --version

section "2. PROJECT ROOT"

pwd
du -sh . 2>/dev/null

section "3. PROJECT DIRECTORY STRUCTURE"

find . -maxdepth 5 \
    \( -name .git -o -name .venv -o -name venv \
       -o -name __pycache__ -o -name node_modules \
       -o -name .pytest_cache -o -name .mypy_cache \) -prune \
    -o -type f -printf '%p (%s bytes)\n' \
    | sort

section "4. GIT STATUS"

if [ -d .git ]; then
    git status --short
    git branch --show-current
    git log -8 --oneline
    git diff --stat
    git ls-files
else
    echo "Git repository not found."
fi

section "5. DOCKER COMPOSE SERVICES"

docker compose config --quiet
echo "Compose validation exit code: $?"

docker compose config --services
docker compose config --images

section "6. DOCKER CONTAINERS STATUS"

docker compose ps -a

section "7. DOCKER RESOURCE USAGE"

docker stats --no-stream 2>/dev/null

section "8. DOCKER VOLUMES"

docker volume ls

section "9. DOCKER NETWORKS"

docker network ls

section "10. PROJECT PYTHON FILES"

find ingestion spark airflow streamlit \
    -type f -name '*.py' 2>/dev/null | sort

section "11. PYTHON FILE LINE COUNTS"

find ingestion spark airflow streamlit \
    -type f -name '*.py' 2>/dev/null \
    -exec wc -l {} + 2>/dev/null

section "12. SQL FILES"

find . -type f \( -name '*.sql' -o -name '*.ddl' \) \
    -not -path '*/.git/*' \
    -not -path '*/.venv/*' 2>/dev/null

section "13. AIRFLOW DAGS"

find airflow/dags -type f 2>/dev/null

section "14. SPARK JOBS"

find spark -type f 2>/dev/null

section "15. INGESTION FILES"

find ingestion -type f 2>/dev/null

section "16. POSTGRES FILES"

find postgres -type f 2>/dev/null

section "17. STREAMLIT FILES"

find streamlit -type f 2>/dev/null

section "18. DEPENDENCIES"

for f in requirements.txt pyproject.toml setup.py \
         ingestion/requirements.txt \
         streamlit/requirements.txt; do
    if [ -f "$f" ]; then
        echo ""
        echo "FILE: $f"
        cat "$f"
    fi
done

section "19. CONFIGURATION FILE NAMES"

find . -maxdepth 4 -type f \
    \( -name '*.yaml' -o -name '*.yml' \
       -o -name '*.toml' -o -name '*.ini' \
       -o -name '*.conf' -o -name 'Dockerfile' \
       -o -name '*.json' \) \
    -not -path '*/.git/*' \
    -not -path '*/node_modules/*' \
    -not -path '*/.venv/*' | sort

section "20. README AND DOCUMENTATION"

find . -maxdepth 4 -type f \
    \( -iname 'readme*' -o -iname '*architecture*' \
       -o -iname '*design*' -o -iname '*documentation*' \
       -o -iname '*schema*' \) \
    -not -path '*/.git/*' \
    -not -path '*/.venv/*'

section "21. TODO AND FIXME"

grep -RInE 'TODO|FIXME|XXX|NotImplementedError' \
    ingestion spark airflow postgres streamlit \
    --exclude-dir=__pycache__ 2>/dev/null | head -300

section "22. DATA DIRECTORY STRUCTURE AND SIZES"

if [ -d data ]; then
    du -h --max-depth=4 data 2>/dev/null | sort -h | tail -100

    find data -maxdepth 5 -type d | sort
else
    echo "No data directory."
fi

section "23. AVAILABLE DISK SPACE"

df -h

section "24. MEMORY"

free -h

section "25. RUNNING PROJECT SERVICES"

for service in postgres kafka mqtt spark-master spark-worker \
               spark-bronze airflow-api-server airflow-scheduler \
               airflow-dag-processor airflow-triggerer streamlit ingestion; do

    echo ""
    echo "========== $service =========="

    docker compose ps "$service" 2>/dev/null

done

section "26. END OF AUDIT"

echo "Report generated: $REPORT"
echo "Review this file before sharing it."
