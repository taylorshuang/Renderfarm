

from DrQueue import Client as DrQueueClient
from DrQueue import Job as DrQueueJob
import time
import pdb

def main():
    client = DrQueueClient()
    while True:
        jobs = DrQueueJob.query_job_status('ready')

        if jobs == []:
            print "Not find any jobs in data base!waiting...."
            time.sleep(60)
            continue

        for job in jobs:
            print "Process job %s" % job["name"]
            try:
                #pdb.set_trace()
                job["job_status"] = u'running'
                DrQueueJob.update_db(job)

                client.job_run(job)
                #print job['job_id']
            except ValueError:
                print "Job's name %s " % job['name']
                print("One of your the specified values produced an error:")
        #time.sleep(10)

            # tasks which have been created
    tasks = client.query_task_list(job['_id'])

    # wait for all tasks of job to finish
    if (tasks == []) and (client.query_computer_list() == []):
        print("Tasks have been sent but no render node is running at the moment.")
        exit(0)

    for task in tasks:
        ar = client.task_wait(task['msg_id'])
        # add some verbose output
        cpl = ar.metadata.completed
        msg_id = ar.metadata.msg_id
        status = ar.status
        engine_id = ar.metadata.engine_id
        print("Task %s finished with status '%s' on engine %i at %i-%02i-%02i %02i:%02i:%02i." % (msg_id, status, engine_id, cpl.year, cpl.month, cpl.day, cpl.hour, cpl.minute, cpl.second))
        if ar.pyerr != None:
            print(ar.pyerr)


    job['job_status'] = 'completed'
    DrQueueJob.update_db(job)
    print("Job %s finished." % job['name'])

if __name__ == "__main__":
    main()
