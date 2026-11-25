from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "schema.sql"


def test_classification_view_is_created_after_fact_and_source_tables():
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in schema.split(";") if statement.strip()]

    fact_table_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("CREATE TABLE IF NOT EXISTS fact_hourly_measurements")
    )
    view_index = next(
        index for index, statement in enumerate(statements)
        if "CREATE OR REPLACE VIEW fact_hourly_measurements_classified" in statement
    )

    assert view_index > fact_table_index
    assert "JOIN dim_source AS s ON s.source_key = f.source_key" in statements[view_index]


def test_classification_keeps_observations_separate_from_model_output():
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in schema.split(";") if statement.strip()]
    view_sql = next(
        statement for statement in statements
        if "CREATE OR REPLACE VIEW fact_hourly_measurements_classified" in statement
    )

    assert "'openaq'" in view_sql and "'esp32_pm_sensor'" in view_sql
    assert "'open_meteo_air_quality'" in view_sql and "'open_meteo_weather'" in view_sql
    assert "THEN 'observation'" in view_sql
    assert "THEN 'model_output'" in view_sql
    assert "ELSE 'unclassified'" in view_sql


def test_schema_semicolon_parser_does_not_split_inside_sql_comments():
    schema = SCHEMA_PATH.read_text(encoding="utf-8")

    assert not any(
        ";" in line.split("--", 1)[1]
        for line in schema.splitlines()
        if "--" in line
    )
