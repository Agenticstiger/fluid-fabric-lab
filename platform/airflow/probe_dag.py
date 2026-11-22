# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The DAG `lab check --tier orchestration` uploads to s3://lab-airflow/dags/lab-check/.

One BashOperator task, run on the scheduler (LocalExecutor) like the tasks of
forge's generated DAGs. It prints a marker `lab check` looks for in the task's
log: a nonce that only this run of the check knows (so an old log cannot pass),
`fluid --version`, and the version of the forge distribution in the image's
venv, which carries the pinned commit (0.32.1.dev0+ge37cdda5). The script is
this file's text with LAB_NONCE replaced; keep the line below as it is.

Airflow 3 imports, as forge's generated DAGs use (airflow.sdk, standard provider).
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

NONCE = "LAB_NONCE"

PRINT_MARKER = (
    "import importlib.metadata as m, json, os; "
    "print('LAB-ORCH ' + json.dumps({'nonce': '" + NONCE + "', 'forge': m.version('data-product-forge'), "
    "'fluid': os.environ.get('FLUID_V'), 'project_dir': os.environ.get('FLUID_PROJECT_DIR'), "
    "'dbt_profiles': os.environ.get('DBT_PROFILES_DIR'), 's3_endpoint': os.environ.get('AWS_ENDPOINT_URL_S3')}))"
)

# `fluid --version` must succeed (the task fails otherwise); its first line goes into the marker.
SCRIPT = (
    'FLUID_V="$(fluid --version 2>&1)" || { echo "fluid --version failed: $FLUID_V"; exit 1; }; '
    'export FLUID_V="$(printf %s "$FLUID_V" | head -n 1)"; '
    'echo "LAB-ORCH-FLUID $FLUID_V"; '
    '/opt/fluid/venv/bin/python -c "$PRINT"'
)

with DAG(
    dag_id="lab_check_orchestration",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["lab-check", "nonce-" + NONCE],  # `lab check` waits for this tag: the listing is THIS upload
    doc_md="Uploaded and removed by `lab check --tier orchestration`; safe to delete.",
) as dag:
    BashOperator(
        task_id="fluid_version",
        # The last line is also the task's XCom `return_value`, so the marker line carries everything.
        bash_command=SCRIPT,
        env={"PRINT": PRINT_MARKER},
        append_env=True,
    )
