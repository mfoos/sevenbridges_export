from sevenbridges import Api
from dotenv import load_dotenv
import os

def write_summary(project, parent_dir):
    # Excludes fields: href, billing group, type, tags, root_folder
    with open(f'{parent_dir}/summary.txt', 'w') as file:
        file.write(f'Project: {project.name}\n')
        file.write(f'Project ID: {project.id}\n')
        file.write(f'Created by: {project.created_by}\n')
        file.write(f'Creation date: {project.created_on}\n')
        file.write(f'Modified: {project.modified_on}\n')
        file.write(f'Description: {project.description}\n')
        file.write(f'Settings:\n{project.settings}\n')

if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("7b-token")
    aapi = Api(url='https://api.sbgenomics.com/v2',
              token=token)

    project = aapi.projects.get(id='companyname/projectname')

    write_summary(project, "testing", aapi)
