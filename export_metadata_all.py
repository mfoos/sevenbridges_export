from sevenbridges import Api
from sevenbridges.http.error_handlers import rate_limit_sleeper, maintenance_sleeper
from pathlib import Path
from dotenv import load_dotenv
import os

from export_tasks import write_tasks
from export_summary import write_summary
from export_apps import write_cwl

load_dotenv()
token = os.getenv("7b-token")
api = Api(url='https://api.sbgenomics.com/v2',
          token=token,
          error_handlers=[rate_limit_sleeper, maintenance_sleeper])

all_projects = list(api.projects.query().all())


def create_directory(project, abs_location):
    short_id = project.id.removeprefix("companyname/")
    Path(f'{abs_location}/{short_id}').mkdir(parents=False, exist_ok=False)
    return f'{abs_location}/{short_id}'


for project in all_projects:
    print(f'Starting project: {project.id.removeprefix("companyname/")}')
    pdir = create_directory(project, "the/path/desired")

    write_summary(project, pdir)
    write_tasks(project, pdir, api)
    write_cwl(project, pdir, api)

    print(f'Project written: {project.id.removeprefix("companyname/")}')
    print(f'Requests Remaining: {api.remaining}')
