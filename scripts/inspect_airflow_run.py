#!/usr/bin/env python3
"""Read-only Airflow DagRun/TaskInstance state inspector."""
import sys
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session

dag_id, run_id = sys.argv[1:3]
with create_session() as session:
    run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one_or_none()
    if run is None:
        raise SystemExit(f"DagRun not found: {dag_id} / {run_id}")
    print(
        f"DAG={run.dag_id} RUN={run.run_id} STATE={run.state} "
        f"START={run.start_date} END={run.end_date}"
    )
    tasks = (
        session.query(TaskInstance)
        .filter_by(dag_id=dag_id, run_id=run_id)
        .order_by(TaskInstance.task_id)
        .all()
    )
    for task in tasks:
        print(
            f"{task.task_id}\t{task.state}\t"
            f"try={task.try_number}\tstart={task.start_date}\tend={task.end_date}"
        )
