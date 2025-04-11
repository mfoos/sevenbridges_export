from sevenbridges import Api
import requests
import json
import time
from itertools import batched
from dotenv import load_dotenv
import os


def write_tasks(project, parent_dir, api):
    tasks = project.get_tasks().all()

    all_the_tasks = []
    for t in tasks:
        all_the_tasks.append(t.id)
        if t.batch:
            children = t.get_batch_children().all()
            [all_the_tasks.append(x.id) for x in children]
            grandchildren = [x.batch for x in children]
            if any(grandchildren):
                print("Grandchild")  # this never happens, only one level of nesting

    with open(f'{parent_dir}/tasks.txt', 'w') as file:
        chunk = list(batched(all_the_tasks, 100))
        got_tasks = []
        for c in chunk:
            got_tasks.extend(api.tasks.bulk_get(tasks=c))
        for bulks in got_tasks:
            t = bulks.resource
            with requests.Session() as s:
                s.headers.update({'X-SBG-Auth-Token': os.getenv("7b-token"), 'accept': 'application/json'})
                file.write(f'\nTask: {t.name}\n')
                file.write(f'App: {t.app}\n')
                file.write(f'Id: {t.id}\n')
                file.write(f'By: {t.executed_by}\n')
                file.write(f'Created: {t.created_time}\n')
                file.write(f'Status: {t.status}\n')
                file.write("Inputs:\n")
                r = s.get(url=f'https://api.sbgenomics.com/v2/tasks/{t.id}/inputs',  
                          params={'fields': '_all'})  
                file.write(json.dumps(json.loads(r.content), indent=4))
                # time.sleep(1)
                file.write('\n')
                file.write("Outputs:\n")
                output_count = api.files.query(
                    project=project.id,
                    origin={
                        'task': t.id
                    }).total  # wow that syntax was hidden from users
                offset = 0
                while offset <= output_count:
                    r = s.get(url=f'https://api.sbgenomics.com/v2/files/',
                              params={'project': project.id, 'origin.task': t.id,
                                      'offset': offset, 'limit': 50})
                    file.write(json.dumps(json.loads(r.content), indent=4))
                    offset += 50
                file.write('\n')


if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("7b-token")
    aapi = Api(url='https://api.sbgenomics.com/v2',
               token=token)

    project = aapi.projects.get(id='company name/projectname')  # small project

    write_tasks(project, "testing", aapi)
