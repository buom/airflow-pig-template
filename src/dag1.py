# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.pig_operator import PigOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import HdfsSensor
from airflow.hooks.pig_hook import PigCliHook
from airflow.contrib.hooks.sqoop_hook import SqoopHook
from airflow.utils.dates import date_range
from airflow.utils.helpers import as_flattened_list
from datetime import timedelta, datetime
import logging as log
import ConfigParser, os


cfg = ConfigParser.RawConfigParser(allow_no_value=True)
cfg.read('{}/config.ini'.format(os.path.dirname(__file__)))

pig_script = open('{}/script.pig'.format(cfg.get("main", "srcdir"))).read()
pig_opts = ['-useHCatalog', '-x', 'mapreduce']



def _pig_prepare_pigparam(d):
    if not d:
        return []
    return as_flattened_list(
        zip(["-p"] * len(d),
	    ['{}={}'.format(k, v) for k, v in d.items()])
    )


def _sqoop_prepare_command(sqoop_cmd_type, hcatalog_database=None, hcatalog_table=None):
    conn_url = 'jdbc:mysql://{}:{}/{}'.format(
        cfg.get("mysql", "host"),
        cfg.get("mysql", "port"),
        cfg.get("mysql", "database")
    )
    connection_cmd = ["sqoop", sqoop_cmd_type]
    connection_cmd += ['-fs', cfg.get("mapred", "namenode")]
    connection_cmd += ['-jt', cfg.get("mapred", "jobtracker")]
    connection_cmd += ['--connect', conn_url]
    connection_cmd += ['--username', cfg.get("mysql", "user")]
    connection_cmd += ['--password-file', 'file:{}/.sqoop/{}.passwd'.format(os.getenv("HOME"), cfg.get("mysql", "database"))]
    if hcatalog_database:
        connection_cmd += ["--hcatalog-database", hcatalog_database]
    if hcatalog_table:
        connection_cmd += ["--hcatalog-table", hcatalog_table]
    return connection_cmd


def _sqoop_eval_cmd(sql):
    cmd = _sqoop_prepare_command('eval')
    cmd += ['-e', '"{}"'.format(sql)]
    return cmd


def _sqoop_export_cmd(
        table,
        hcatalog_table,
        hcatalog_partition_keys,
        hcatalog_partition_values,
        columns,
        update_key=None,
        update_mode='allowinsert',
        skip_dist_cache=True):
    hcatalog_database = cfg.get("hcatalog", "database")
    cmd = _sqoop_prepare_command('export', hcatalog_database, hcatalog_table)
    cmd += ['--table', table]
    cmd += ['--columns', columns]
    cmd += ['--hcatalog-partition-keys', hcatalog_partition_keys]
    cmd += ['--hcatalog-partition-values', hcatalog_partition_values]
    cmd += ['--update-mode', update_mode]
    if update_key:
        cmd += ['--update-key', update_key]
    if skip_dist_cache:
        cmd += ['--skip-dist-cache']
    return cmd


def sqoop_delete(
        ds,
        table,
        **kwargs):
    prod_code = cfg.get("others", "prod_code")
    log_date = airflow.macros.ds_add(ds, -1)
    sql = """DELETE FROM {} WHERE log_date='{}' AND product_code='{}'""".format(table, log_date, prod_code)
    cmd = _sqoop_eval_cmd(sql)
    h = SqoopHook()
    h.Popen(cmd)


def sqoop_export(
        ds,
        table,
        hcatalog_table,
        hcatalog_partition_keys,
        columns,
        update_key,
        **kwargs):
    hcatalog_partition_values = kwargs['templates_dict']['hcatalog_partition_values']
    cmd = _sqoop_export_cmd(table,
                            hcatalog_table,
                            hcatalog_partition_keys,
                            hcatalog_partition_values,
                            columns,
                            update_key=update_key)
    log.info(' '.join(cmd))
    h = SqoopHook()
    h.Popen(cmd)


def pig_run(
        ds,
        **kwargs):
    pig_params = _pig_prepare_pigparam({
        'prodCode': cfg.get("others", "prod_code"),
        'db': cfg.get("hcatalog", "database"),
        'inputPath': cfg.get('main', 'input_path'),
        'ds': '"ds={}"'.format(airflow.macros.ds_add(ds, -1)),
    })
    opts = ' '.join(pig_opts + pig_params)
    kwargs['ti'].xcom_push(key='PIG_OPTS', value=opts)
    h = PigCliHook()
    h.run_cli(pig=pig_script, pig_opts=opts)


args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    #'end_date': datetime(2018, 6, 26, 16),
}


dag = DAG(
    dag_id='dag1',
    default_args=args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    #schedule_interval=timedelta(minutes=5),
    #end_date=datetime(2018, 6, 23),
    schedule_interval=None)


t9 = PythonOperator(
    task_id='pig_run',
    provide_context=True,
    python_callable=pig_run,
    dag=dag)


t8 = PythonOperator(
    task_id='sqoop_delete',
    provide_context=True,
    python_callable=sqoop_delete,
    op_kwargs={'table': 'pu'},
    dag=dag)

t7 = PythonOperator(
    task_id='sqoop_export',
    provide_context=True,
    python_callable=sqoop_export,
    op_kwargs={
        'table': 'pu',
        'hcatalog_table': 'pu',
        'hcatalog_partition_keys': '"ds"',
        'columns': 'log_date,product_code',
        'update_key': 'log_date,product_code',
    },
    templates_dict={'hcatalog_partition_values': '"{{ macros.ds_add(ds, -1) }},%s"' % cfg.get("others", "prod_code")},
    dag=dag)


t9 >> t8 >> t7
