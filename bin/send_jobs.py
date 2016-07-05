# -*- coding: utf-8 -*-

"""
Create new job
Copyright (C) 2011 Andreas Schroeder

This file is part of DrQueue.

Licensed under GNU General Public License version 3. See LICENSE for details.
"""

from optparse import OptionParser
import os
import DrQueue
from DrQueue import Job as DrQueueJob
from DrQueue import Client as DrQueueClient
import getpass
import uuid


def main():
    # parse arguments
    parser = OptionParser()
    parser.usage = "%prog [options] -n name -r renderer -f scenefile"
    parser.add_option("-s", "--startframe",
                      dest="startframe", default=1, help="first frame")
    parser.add_option("-e", "--endframe",
                      dest="endframe", default=1, help="last frame")
    parser.add_option("-b", "--blocksize",
                      dest="blocksize", default=1, help="size of block")
    parser.add_option("-y", "--priority",
                      dest="priority", default=1, help="priority of each block")
    parser.add_option("-n", "--name",
                      dest="name", default=None, help="name of job")
    parser.add_option("-r", "--renderer",
                      dest="renderer", help="render type (maya|blender|mentalray)")
    parser.add_option("-f", "--scenefile",
                      dest="scenefile", default=None, help="path to scenefile")
    parser.add_option("-p", "--pool",
                      dest="pool", default=None, help="pool of computers")
    parser.add_option("-o", "--options",
                      dest="options", default="{}", help="specific options for renderer as Python dict")
    parser.add_option("--retries",
                      dest="retries", default=1, help="number of retries for every task")
    parser.add_option("--owner",
                      dest="owner", default=getpass.getuser(), help="Owner of job. Default is current username.")
    parser.add_option("--os",
                      dest="os", default=None, help="Operating system.")
    parser.add_option("--minram",
                      dest="minram", default=0, help="Minimal RAM in GB.")
    parser.add_option("--mincores",
                      dest="mincores", default=0, help="Minimal CPU cores.")
    parser.add_option("--send-email",
                      action="store_true", dest="send_email", default=False, help="Send notification email when job is finished.")
    parser.add_option("--email-recipients",
                      dest="email_recipients", default=None, help="Recipients for notification email.")
    parser.add_option("-w", "--wait",
                      action="store_true", dest="wait", default=False, help="wait for job to finish")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False, help="verbose output")
    (options, args) = parser.parse_args()

    # set limits
    limits = dict()
    limits['pool_name'] = options.pool
    limits['os'] = options.os
    limits['minram'] = int(options.minram)
    limits['mincores'] = int(options.mincores)

    options_var = eval(options.options)
    options_var['send_email'] = options.send_email
    options_var['email_recipients'] = options.email_recipients

    # add default rendertype if missing
    if "rendertype" not in options_var:
        options_var['rendertype'] = "animation"

    # initialize DrQueue job
    job = DrQueueJob(options.name, int(options.startframe), int(options.endframe), int(options.blocksize),int(options.priority), options.renderer, options.scenefile, options.retries, options.owner, options_var, "send_job.py", limits)

    # save job in database
    job['job_status'] = 'ready'

    # check the condition of each job
    # check job name
    if job['name'] in DrQueueJob.query_jobnames():
        raise ValueError("Job name %s is already used!" % job['name'])

    # check frame numbers
    if not (job['startframe'] >= 0):
        raise ValueError("Invalid value for startframe. Has to be equal or greater than 1.")

    if not (job['endframe'] >= 0):
        raise ValueError("Invalid value for endframe. Has to be equal or greater than 1.")

    if not (job['endframe'] >= job['startframe']):
        raise ValueError("Invalid value for endframe. Has be to equal or greater than startframe.")

    if job['endframe'] > job['startframe']:
        if not (job['endframe'] - job['startframe'] >= job['blocksize']):
            raise ValueError("Invalid value for blocksize. Has to be equal or lower than endframe-startframe.")

    if job['endframe'] == job['startframe']:
        if job['blocksize'] != 1:
            raise ValueError("Invalid value for blocksize. Has to be equal 1 if endframe equals startframe.")

    if job['name'] == "":
        raise ValueError("No name of job given!")
    if DrQueue.check_renderer_support(job['renderer']) == False:
        raise ValueError("Render called \"%s\" not supported!" % job['renderer'])
    if job['scenefile'] == "":
        raise ValueError("No scenefile given!")
    # save job
    DrQueueJob.store_db(job)

if __name__ == "__main__":
    main()


