# This file would be helpful for a subdag but is not used in main dag.

import os
from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_subdag(parent_dag_name,
                child_dag_name,
                dim_tables,
                default_args):

    dag_subdag = DAG(
        dag_id = f'{parent_dag_name}.{child_dag_name}',
        default_args = default_args
    )

    with dag_subdag:
        for tbl in dim_tables:
            t = LoadDimensionOperator(task_id = f'load_{target_table}_dim_table',
                                      dag = dag_subdag,
                                      redshift_conn_id = 'redshift',
                                      target_table = tbl,
                                      sql = SqlQueries.dim_tables_map[tbl],
                                      refresh_table = False)

    return dag_subdag
