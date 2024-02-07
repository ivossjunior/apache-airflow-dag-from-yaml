from dag_generator_from_yaml import DagGeneratorFromYaml

DAG_FOLDER = 'C:\\Projetos\\apache-airflow-dag-from-yaml\\dags\\'
TEMPLATE_FOLDER = 'C:\\Projetos\\apache-airflow-dag-from-yaml\\templates\\'

dag_generator = DagGeneratorFromYaml(dag_template_name = 'dag.template', dag_name = f'{DAG_FOLDER}dag-example.py', templates_path = TEMPLATE_FOLDER)
dag_generator.read_yaml('dags\\dag01.yaml')
dag_generator.generate_dag()
