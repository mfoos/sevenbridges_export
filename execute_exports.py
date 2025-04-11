from sevenbridges import Api
from sevenbridges.http.error_handlers import rate_limit_sleeper, maintenance_sleeper
from dotenv import load_dotenv
import os
import time
from export_files import export_from_manifest
from datetime import datetime

load_dotenv()
token = os.getenv("7b-token")

api = Api(url='https://api.sbgenomics.com/v2',
          token=token,
          error_handlers=[rate_limit_sleeper, maintenance_sleeper],
          advance_access=True)

exports_ct = 0
with open(f'{parent_dir}/files_retry.txt') as file:
    for row in file:
        #count, project = row.split()
        project = row.rstrip()
        shortname = f'companyname/{project}'
        shortername = project
        #shortname = f'companyname/{project.removesuffix("/file_manifest.txt")}'
        #shortername = shortname.removeprefix('companyname/')
        if exports_ct < 25:
            if not os.path.exists(f'{parent_dir}/{shortername}/export_log.txt'):
                print(f'\n> Exporting {shortername}')
                print("5 safe seconds to kill process")
                time.sleep(5)  # give me a chance to cancel
                print(f'running at {datetime.now().strftime('%H:%M:%S')}\n')
                project = api.projects.get(id=shortname)
                export_from_manifest(project, parent_dir, api)
                exports_ct += 1
            else:
                print(f'{shortername} has already been exported')
print(f'Exported this run: {exports_ct}')