from airflow.models import DagBag
import os
from airflow.exceptions import AirflowBadRequest


def file_system_persist_function(file_name, payload):
    fd = open(file_name, "w")
    fd.write(payload)
    fd.close()


def update_add_dag_from_payload(file_name, file_payload, persist_func=file_system_persist_function):
    if "DAG" in file_payload and "airflow" in file_payload:
        dag_bag = DagBag()
        file_prefix = os.path.splitext(file_name)[0]
        full_file_path = "%s/%s%s" % (str(dag_bag.dag_folder), file_prefix, ".py")
        persist_func(file_name=full_file_path, payload=file_payload)
        return full_file_path
    else:
        raise AirflowBadRequest("Dag File is not valid airflow code:"
                                "%s" % file_payload)
