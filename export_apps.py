from sevenbridges import Api
import requests
import json
import time
from dotenv import load_dotenv
import os

def write_cwl(project, parent_dir, api):
    project_id = project.id
    short_id = project.id.removeprefix("companyname/")
    apps = project.get_apps().all() # don't get screwed by pagination
    for a in apps:
        app_id = a.id.removeprefix(project_id+"/")
        # Do I need to "Get details" or is that redundant to CWL?
        # CWL seems to contain project name, id and app revision
        with open(f'{parent_dir}/{app_id}.cwl', 'w') as file:
            with requests.Session() as s:
                s.headers.update({'X-SBG-Auth-Token': os.getenv("7b-token"), 'accept': 'application/json'})
                r = s.get(url=f'https://api.sbgenomics.com/v2/apps/companyname/{short_id}/{app_id}/raw')
                # error catching here
                file.write(json.dumps(json.loads(r.content), indent=4))
            time.sleep(2)

if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("7b-token")
    aapi = Api(url='https://api.sbgenomics.com/v2',
              token=token)

    project = aapi.projects.get(id='companyname/projectname')

    write_cwl(project, "testing", aapi)