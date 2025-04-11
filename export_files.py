from datetime import datetime
from sevenbridges import Api
from sevenbridges.http.error_handlers import rate_limit_sleeper, maintenance_sleeper
from dotenv import load_dotenv
from pathlib import Path
from itertools import batched
import os
import csv
import re
import time

# Steps: 1) Traverse files, 2) Map ids to human-readable paths, 3) File manifest 4) Run exports


def write_file_manifest(project, parent_dir, aapi):
    def capture_files(project):
        keepfiles = []
        files_metadata = {}
        folders_ids_names = {}
        root = aapi.files.get(id=project.root_folder)

        def dig_in(folder):
            #print(f'Recursing on: {folder.name}')
            nonlocal keepfiles
            nonlocal files_metadata
            nonlocal folders_ids_names
            if folder.is_folder():
                folders_ids_names[folder.id] = folder.name
                keepfiles.append([folder.id, folder.parent, "folder"])
                for f in folder.list_files().all():
                    dig_in(f)
            else:
                files_metadata[folder.id] = [folder.id, folder.name, folder.storage.type, folder.storage.volume,
                                             folder.storage.location]
                keepfiles.append([folder.id, folder.parent, folder.name])
        dig_in(root)
        del (folders_ids_names[root.id])  # the "name" of root is not human-useful
        return keepfiles, files_metadata, folders_ids_names

    id_parent_name, file_metadata, translate = capture_files(project)

    # cycle through and place things in a new structure
    s3_paths = {}
    file_iterator = []
    for child, parent, filename in id_parent_name:
        if parent is None:
            # root directory
            s3_paths[child] = child
        elif filename != "folder":
            # if child is a terminal file
            # child and filename are the same file by different identifiers
            minilist = f'{s3_paths[parent]};{filename}'
            s3_paths[child] = minilist
            file_iterator.append(child)
        else:
            # if child is a folder
            minilist = f'{s3_paths[parent]};{child}'
            s3_paths[child] = minilist

    # Create a manifest text file
    # UID, human filename, original 7B path, alias AWS location
    with open(f'{parent_dir}/file_manifest.txt', 'w') as file:
        file.write("\t".join(["file id", "file name", "path", "bucket where aliased", "location on bucket"]) + "\n")
        for f in file_iterator:
            uid, hrfilename, storage, alias_bucket, alias_prefix = file_metadata[f]
            path_7b = "/" + "/".join([translate.get(x, x) for x in s3_paths[f].split(";")][1:])
            row = [f, hrfilename, path_7b.removesuffix(hrfilename), str(alias_bucket), str(alias_prefix)]
            file.write("\t".join(row) + "\n")


def create_directory(project, abs_location):
    short_id = project.id.removeprefix("companyname/")
    Path(f'{abs_location}/{short_id}').mkdir(parents=False, exist_ok=False)
    return f'{abs_location}/{short_id}'


def export_from_manifest(project, manifest_dir, aapi):
    short_id = project.id.removeprefix("companyname/")
    logfile = f'{manifest_dir}/{short_id}/export_log.txt'
    if os.path.exists(logfile):
        logfile = f'{manifest_dir}/{short_id}/export_log_{datetime.now().strftime('%d%b%Y_%H%M%S')}.txt'

    tmpfile = f'{manifest_dir}/{short_id}/log_tmp.txt'

    with open(f'{manifest_dir}/{short_id}/file_manifest.txt', 'r') as manfile:
        manifest = csv.reader(manfile, delimiter="\t")
        next(manifest)

        export_volume = aapi.volumes.get('companyname/7bridgesbbb')
        exports = []
        for uid, fn, path7b, alias_b, alias_p in manifest:  # this loop never runs if there's no data
            if alias_b == "None":  # aliased files do not need to be exported, and in fact error
                #print(f'Preparing to export {fn}')
                export = {
                    'file': uid,
                    'volume': export_volume,
                    'location': re.sub("/+", "/", f'00_Oct2024_7B_export/{short_id}/files/{path7b}/{fn}')
                }

                exports.append(export)  # Max files per export is 100
        response_list = []
        print(f'{len(exports)} files to export')
        chunk = list(batched(exports, 100))
        with open(tmpfile, 'w') as tmpfn:
            for c in chunk:
                rec = aapi.exports.bulk_submit(exports=c, copy_only=False)
                print("Submitting chunk")
                response_list.extend(rec)
                time.sleep(0.5)
                eids = []
                for r in rec:
                    rr = r.resource
                    if r.valid:
                        eids.append(rr.id)
                    else:
                        eids.append("invalid")
                all_ids = list(zip([exp['file'] for exp in c], eids))
                tmpfn.write("\n".join(["\t".join(x) for x in all_ids]) + "\n")
        print("All exports submitted")
        # put logging here to protect during error
        # bc it's bulk, error prevention super important :headdesk:

    # want logging but also need periodic status check
    # this is the section that gobbles up API calls
    # this still runs if every single file is invalid, which is stupid
    # this gets slow as early as like 2-4k files
    with open(logfile, 'w') as log:
        headers = ["time", "is_valid", "export_id", "file_id", "status", "error"]
        log.write("\t".join(headers)+"\n")
        # for export_record in response_list:
        #     time.sleep(0.5)
        #     if export_record.valid:
        #         export = export_record.resource
        #         initial = [datetime.now().strftime('%d%b%Y [%H:%M:%S]'),
        #                    str(export_record.valid),
        #                    export.id,
        #                    export.source.id,
        #                    export.state]
        #         if export.error is not None:
        #             initial.append(f'Error {export.error.code}: {export.error.message}')
        #     else:
        #         initial = [datetime.now().strftime('%d%b%Y [%H:%M:%S]'), str(export_record.valid),
        #                    "no export_id", "no uid", "invalid",
        #                    f'Error {export_record.error.code}: {export_record.error.message}']
        #         print(">>>>>> Invalid submission")
        #         # reasons for invalid submission: Archived files, freshly restored files?, files in a different AWS region, "public"/7B files
        #     log.write("\t".join(initial) + "\n")
        #     log.flush()

        # # Don't let this run on invalid files
        # total_files = len(response_list)
        # resolved_files = 0
        # while resolved_files < total_files:
        #     print(len(response_list))
        #     for export_record in response_list:
        #         time.sleep(0.5)
        #         if export_record.valid:
        #             export = export_record.resource
        #             reloaded = export.reload()
        #             subsequent = [datetime.now().strftime('%d%b%Y [%H:%M:%S]'),
        #                           str(export_record.valid),
        #                           export.id,
        #                           export.source.id,
        #                           reloaded.state]
        #             if export.error is not None:
        #                 subsequent.append(f'Error {reloaded.error.code}: {reloaded.error.message}')
        #             log.write("\t".join(subsequent) + "\n")
        #             log.flush()
        #             if reloaded.state in ['COMPLETED', 'FAILED']:
        #                 response_list.remove(export_record)
        #                 resolved_files += 1
        #         else:
        #             resolved_files += 1  # invalid submissions are resolved


if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("7b-token")
    api = Api(url='https://api.sbgenomics.com/v2',
              token=token,
              error_handlers=[rate_limit_sleeper, maintenance_sleeper],
              advance_access=True)

    project = api.projects.get(id='companyname/mfoos-file-export-test')  # small project

    pdir = create_directory(project, "the/path/desired")
    write_file_manifest(project, pdir, api)



