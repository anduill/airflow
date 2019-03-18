import unittest

from airflow import models
from airflow import settings
from airflow.api.common.experimental import add_update_dag
from airflow.exceptions import AirflowBadRequest

test_payload = """from airflow import DAG
from airflow.contrib.sensors.qubole_sensor import QuboleFileSensor, QubolePartitionSensor
from airflow.utils import dates

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('example_qubole_sensor', default_args=default_args, schedule_interval=None)

dag.doc_md = __doc__

t1 = QuboleFileSensor(
    task_id='check_s3_file',
    qubole_conn_id='qubole_default',
    poke_interval=60,
    timeout=600,
    data={
        "files":
            [
                "s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar",
                "s3://paid-qubole/HadoopAPITests/data/{{ ds.split('-')[2] }}.tsv"
            ]  # will check for availability of all the files in array
    },
    dag=dag
)

t2 = QubolePartitionSensor(
    task_id='check_hive_partition',
    poke_interval=10,
    timeout=60,
    data={"schema": "default",
          "table": "my_partitioned_table",
          "columns": [
              {"column": "month", "values":
                  ["{{ ds.split('-')[1] }}"]},
              {"column": "day", "values":
                  ["{{ ds.split('-')[2] }}", "{{ yesterday_ds.split('-')[2] }}"]}
          ]  # will check for partitions like [month=12/day=12,month=12/day=13]
          },
    dag=dag
)

t1.set_downstream(t2)"""

class MockFileSystem:
    def __init__(self):
        self.file_name = None
        self.payload = None

    def file_system_persist_function(self,file_name, payload):
        self.file_name = file_name
        self.payload = payload


class TestAddDag(unittest.TestCase):

    def test_add_dag_exception(self):
        test_file_name = "file_1"
        bad_payload = "bad payload"
        try:
            mock_file_system = MockFileSystem()
            add_update_dag.update_add_dag_from_payload(file_name=test_file_name, file_payload=bad_payload, persist_func=mock_file_system.file_system_persist_function)
            self.assertTrue(False)
        except AirflowBadRequest as e:
            self.assertTrue(True)

    def test_add_dag_1(self):
        mock_file_system = MockFileSystem()
        test_file_name = "file_1"
        add_update_dag.update_add_dag_from_payload(file_name=test_file_name, file_payload=test_payload, persist_func=mock_file_system.file_system_persist_function)
        file_tokens = mock_file_system.file_name.split("/")
        self.assertEqual("file_1.py", file_tokens[len(file_tokens) -1])
        self.assertEqual(test_payload, mock_file_system.payload)

    def test_add_dag_2(self):
        mock_file_system = MockFileSystem()
        test_file_name = "file_1.py"
        add_update_dag.update_add_dag_from_payload(file_name=test_file_name, file_payload=test_payload, persist_func=mock_file_system.file_system_persist_function)
        file_tokens = mock_file_system.file_name.split("/")
        self.assertEqual("file_1.py", file_tokens[len(file_tokens) -1])
        self.assertEqual(test_payload, mock_file_system.payload)

if __name__ == '__main__':
    unittest.main()
