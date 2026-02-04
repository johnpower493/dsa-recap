Analytical Data Modelling (Exercises)

This folder contains hands-on exercises for common analytical modelling methodologies.

All SQL exercises are written for Postgres and assume the demo schema in:
  sql/00_setup_schema_and_seed.sql

Recommended order:
  1) 01_dimensional_star_schema_exercise.sql
  2) 01_dimensional_star_schema_solution.sql
  3) 02_snowflake_vs_star_exercise.sql
  4) 02_snowflake_vs_star_solution.sql
  5) 03_factless_facts_and_bridges_exercise.sql
  6) 03_factless_facts_and_bridges_solution.sql
  7) 04_aggregate_facts_and_rollups_exercise.sql
  8) 04_aggregate_facts_and_rollups_solution.sql
  9) 05_data_vault_2_0_exercise.sql
  10) 05_data_vault_2_0_solution.sql
  11) 06_one_big_table_obt_exercise.sql
  12) 06_one_big_table_obt_solution.sql
  13) 07_kimball_bus_conformed_dims_exercise.sql
  14) 07_kimball_bus_conformed_dims_solution.sql
  15) 08_scd_type2_dim_history_exercise.sql
  16) 08_scd_type2_dim_history_solution.sql
  17) 09_metrics_semantic_layer_exercise.sql
  18) 09_metrics_semantic_layer_solution.sql

How to run (psql):
  \i sql/00_setup_schema_and_seed.sql
  \i data_modelling/01_dimensional_star_schema_solution.sql

Notes:
- Exercises are written as TODO blocks. Solutions are fully runnable.
- The goal is to practice modelling choices and the SQL that operationalizes them.
