This is not the tightest code that has ever been written. I was iterating rapidly, sometimes modifying on the fly after the 1001st file suddenly errored out. When I was facing down a deadline to preserve all of our data when the license ended, I was surprised there was no "Export project" code, so I wrote it. I hope it will save others time.

execute_exports.py - Runs the file exports, can be set (ln 26) to only run a few at a time

export_apps.py - Exports the CWL from 7B "Apps"

export_files.py - Bulk exports files by copying them to S3. Recursively traverses file structure so if you have a very deep hierarchy, you will be sad (and probably mad)

export_metadata_all.py - Runs all the other exports, which is more straightforward than the files

export_summary.py - Exports the high level project information

export_tasks.py - Exports tasks, including the subtasks of batches, because if you're trying to match an output with a task id, you need access to the subtasks
