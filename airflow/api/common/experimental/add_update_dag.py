from airflow.models import DagBag
import os
from airflow.exceptions import AirflowBadRequest
def update_add_dag_from_payload(file_name, file_payload):
    if "DAG" in file_payload and "airflow" in file_payload:
        dag_bag = DagBag()
        file_prefix = os.path.splitext(file_name)[0]
        full_file_path = "%s/%s%s" % (str(dag_bag.dag_folder), file_prefix, ".py")
        fd = open(full_file_path, "w")
        fd.write(file_payload)
        fd.close()
        return full_file_path
    else:
        raise AirflowBadRequest("Dag File is not valid airflow code:"
                                "%s" % file_payload)
