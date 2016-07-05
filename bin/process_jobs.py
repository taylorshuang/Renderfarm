__author__ = 'shuang'


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


if __name__ == "__main__":
    main()
