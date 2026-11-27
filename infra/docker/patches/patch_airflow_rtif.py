"""Apply a narrow retry for concurrent Airflow RTIF primary-key races.

This is a local compatibility patch for the pinned Airflow 3.0.6 image.
It only retries the specific rendered_task_instance_fields primary-key
collision; all other IntegrityError exceptions are re-raised unchanged.
"""
from pathlib import Path

import airflow

route = (
    Path(airflow.__file__).parent
    / "api_fastapi"
    / "execution_api"
    / "routes"
    / "task_instances.py"
)
source = route.read_text(encoding="utf-8")

old_import = "from sqlalchemy.exc import NoResultFound, SQLAlchemyError"
new_import = "from sqlalchemy.exc import IntegrityError, NoResultFound, SQLAlchemyError"
if source.count(old_import) != 1:
    raise SystemExit("Expected exactly one Airflow 3.0.6 SQLAlchemy exception import.")
source = source.replace(old_import, new_import, 1)

old_block = """    task_instance.update_rtif(put_rtif_payload, session)
    log.debug("RenderedTaskInstanceFields updated successfully")

    return {"message": "Rendered task instance fields successfully set"}
"""
new_block = """    try:
        task_instance.update_rtif(put_rtif_payload, session)
    except IntegrityError as exc:
        # session.merge() is SELECT-then-INSERT. Concurrent RTIF requests can
        # both observe no row and race on the RTIF primary key. Once the peer
        # commits, retrying after rollback turns merge() into an UPDATE.
        original = getattr(exc, "orig", None)
        diagnostic = getattr(original, "diag", None)
        constraint_name = getattr(diagnostic, "constraint_name", None)
        if (
            constraint_name != "rendered_task_instance_fields_pkey"
            and "rendered_task_instance_fields_pkey" not in str(original)
        ):
            raise

        session.rollback()
        task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
        if not task_instance:
            log.error("Task Instance disappeared during RTIF conflict retry")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

        # The first transaction was rolled back; this second write sees the
        # row committed by the concurrent request and safely updates it.
        task_instance.update_rtif(put_rtif_payload, session)

    log.debug("RenderedTaskInstanceFields updated successfully")
    return {"message": "Rendered task instance fields successfully set"}
"""
if source.count(old_block) != 1:
    raise SystemExit("Expected exactly one unpatched ti_put_rtif block.")
source = source.replace(old_block, new_block, 1)

route.write_text(source, encoding="utf-8")
compile(source, str(route), "exec")
print(f"Patched {route}")
