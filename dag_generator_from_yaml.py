from jinja2 import Template
import yaml
import os 

class DagGeneratorFromYaml:
    def __init__(self, dag_template_name: str, dag_name: str, templates_path: str):
        """ 
        Class responsible for generate DAGs from YAML files.

        Parameters:
        dag_template_name (str): Template's name that will be used to generate a DAG from a YAML file.
        dag_name (str): DAG's path that will be generated.
        templates_path (str): Path where the templates are located. 
        
        """
        self.template_name = dag_template_name
        self.dag_name = dag_name
        self.yaml_file = None
        self.operators = ''
        self.dependencies = ''
        self.templates_path = templates_path
        self.libraries = ''

    def read_yaml(self, dag_template_path: str) -> None:
        """
        Reads the YAML file.

        Parameters:
        dag_template_path (str): Path where the DAG's template is located.

        Returns:
        None
        """
        with open(dag_template_path, "r") as yaml_file:
            self.yaml_file = yaml.safe_load(yaml_file)

    def generate_library_dependencies(self) -> None:
        """
        Adds the library dependecies in the Python file.

        Parameters:
        --

        Returns:
        None        
        """
        for library in self.yaml_file['libraries_dependency']:
            if library['alias'] is not None or library['alias'] != '':
                self.libraries += f'import {library["name"]} as {library["alias"]} \n'
            else:
                self.libraries += f'import {library["name"]} \n'

    def generate_dependencies(self, actual_task_id, task_list) -> None:
        """
        Creates the dependecy list between tasks.

        Parameters:
        actual_task_id (str): The task id that will identify the dependencies.
        task_list (list): The task list declared on YAML file.

        Returns:
        None
        """
        dependencies = []
        for task in task_list:
            if 'dependencies' in task:
                if actual_task_id == task['dependencies'][0]:
                    dependencies.append(task['task_id'])

        if len(dependencies) > 0:
            _dependencies = f'{actual_task_id} >> ['

            for task_id in dependencies:
                _dependencies += task_id if task_id == dependencies[-1] else task_id + ', '
        
            _dependencies += ']\n'

            return _dependencies
    
    def generate_tasks(self) -> None:
        """
        Generates the task list in the Python file.

        Parameters:
        ---

        Returns:
        None
        """
        for task in self.yaml_file['tasks']:
            library = self.get_library(task['operator_template'])

            if library not in self.libraries:
                self.libraries += library

            with open(f'{self.templates_path}\\{task["operator_template"]}') as template_file:
                
                template = Template(template_file.read())
                self.operators += template.render(task = task)
                self.operators += '\n\n'

            _dependencies = self.generate_dependencies(task['task_id'], self.yaml_file['tasks'])

            self.dependencies += _dependencies if _dependencies is not None else ''

    def get_library(self, operator_name) -> None:
        """
        Get the Operator's import path.

        Parameters:
        operator_name (str): Name of the operator

        Returns:
        None
        """
        with open(f'libs\\{operator_name}') as lib:
            return lib.readline() + '\n'

    def render_template(self) -> None:
        """
        Render the templates and writes a DAG file using the dag_name parameter.

        Parameters:
        ---

        Returns:
        None
        """
        with open(f"templates\\{self.template_name}", "r") as template_file:
            template = Template(template_file.read())

        with open(self.dag_name, "w") as create_dag:
            create_dag.write(self.libraries)
            create_dag.write(template.render(config=self.yaml_file))
            create_dag.write(self.operators)
            create_dag.write(self.dependencies)
        
    def generate_dag(self) -> None:
        """
        Generates the dag.

        Parameters:
        ---

        Return:
        None
        """
        self.generate_tasks()
        self.generate_library_dependencies()
        self.render_template()

DAG_FOLDER = 'C:\\Projetos\\Estudos\\apache-airflow\\airflow\\dags\\'
TEMPLATE_FOLDER = 'C:\\Projetos\\Estudos\\apache-airflow\\dag-generator-from-yaml\\templates\\'

dag_generator = DagGeneratorFromYaml(dag_template_name = 'dag.template', dag_name = f'{DAG_FOLDER}dag-example.py', templates_path = TEMPLATE_FOLDER)
dag_generator.read_yaml('dags\\dag01.yaml')
dag_generator.generate_dag()
