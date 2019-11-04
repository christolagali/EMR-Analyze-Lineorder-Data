
"""
Organize Imports

"""

import argparse
import importlib
import os
import sys


if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


try:
    import pyspark

except:
    import findspark
    findspark.init()
    import pyspark

from pyspark.sql import SparkSession


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run a Pyspark Job")
    parser.add_argument('--job',type=str,required=True,dest='job_name',help="The name of the job module you want to run")
    parser.add_argument('--job-args',nargs='*',help='Extra araguments to send to the job')

    args = parser.parse_args()

    print('Called with arguments : %s' %args)

    job_args = dict()

    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print('job args tuples: %s ' %job_args_tuples)
        job_args = {a[0]:a[1] for a in job_args_tuples}
    
     # initialize SparkContext
    sc = pyspark.SparkContext(appName=args.job_name)

    sparksession = SparkSession.builder.appName(args.job_name).getOrCreate()

    job_module = importlib.import_module('jobs.%s' % args.job_name)

    job_module.analyze(sc,sparksession)
    


