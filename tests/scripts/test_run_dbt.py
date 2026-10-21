from scripts.run_dbt import DBT_DIR, dbt_args


def test_dbt_args_point_at_the_project_and_profile():
    args = dbt_args(["build", "--select", "fct_lot_quality"])
    assert args[:3] == ["build", "--select", "fct_lot_quality"]
    assert args[args.index("--project-dir") + 1] == str(DBT_DIR)
    assert args[args.index("--profiles-dir") + 1] == str(DBT_DIR)
    assert (DBT_DIR / "dbt_project.yml").exists()
