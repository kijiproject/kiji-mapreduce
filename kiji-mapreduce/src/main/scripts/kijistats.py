'''
Copyright 2013 WibiData, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
import os
import re
import sys

"""
Positional constants for information
"""
JOB_NAME = 0
JOB_ID = 1
TASK_ATTEMPT = 2
FUNC_SIG = 3
AGGREGATE_TIME = 4
INVOCATIONS = 5
PER_CALL_TIME = 6

"""
Graph colors
"""
COLOR_AGGR = '#cc99ff'
COLOR_INV = '#ff99ff'
COLOR_PCT = '#cc0099'

'''
    kijistats analyzes the data collected by profiling KijiSchema and
    KijiMR during a map reduce job.
'''

def BuildFlagParser():
    """Flag parses for the Kiji stats tool.

    Returns:
    Command-line flag parser.
    """
    parser = argparse.ArgumentParser(
        description='Kiji Stats tool to analyze profiling data.'
    )
    parser.add_argument(
        '--stats-dir',
        dest='stats_dir',
        type=str,
        default=os.getcwd(),
        help='Local directory where profiling data is stored. ' +
        'Usually called `kijistats` in the working directory of the map reduce job. ' +
        'You will need to copy this directory on hdfs to your local filesystem and supply it.'
    )
    jobgrp = parser.add_mutually_exclusive_group(required=False)
    jobgrp.add_argument(
        '--by-job',
        dest='job',
        type=str,
        help='Job ID for which to collect stats.'
    )
    jobgrp.add_argument(
        '--by-jobname',
        dest='jobname',
        type=str,
        help='Name of the job for which to collect stats. This will be ignored if' +
        ' the job ID has been specified.' +
        ' Note that multiple jobs may have the same name. Example: MapFamilyGatherer.'
    )
    jobgrp.add_argument(
        '--by-task',
        dest='taskid',
        type=str,
        help='Task attempt ID for which to collect stats.'
    )
    parser.add_argument(
       '--by-function',
       dest='function_name',
       type=str,
       help='Function for which to collect stats. Can be used to collect stats about' +
       ' this function across all task attempts or jobs or for a single task or job.'
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('--as-graph', action='store_true')
    grp.add_argument('--to-file', type=str,
                     help='File name to store aggregated result.')
    return parser


def gatherData(flags):
    stats_dir = flags.stats_dir
    taskid = flags.taskid
    jobid = flags.job
    jobname = flags.jobname
    function_name = flags.function_name
    profile_data = []
    if not taskid:
        # Aggregate all files under the stats directory
        for filename in os.listdir(stats_dir):
            filename = os.path.join(stats_dir, filename)
            if os.path.isfile(filename) and not filename.startswith(".") \
                and not filename.endswith("crc"):
                with open(filename) as f:
                    for line in f.readlines():
                        if line.startswith('Job Name, Job ID,'):
                            continue
                        # We split first so that we dont accidentally match jobab
                        # to jobabc in the string form.
                        # We need regular expressions because otherwise we might
                        # split on commas in function signatures
                        r = re.compile(r'(?:[^,(]|\([^)]*\))+')
                        splitline = [x.strip() for x in r.findall(line)]
                        if len(splitline) > PER_CALL_TIME + 1:
                            raise RuntimeError('Possible error in input format')
                        # Filtering on job id
                        if jobid:
                            if  jobid == splitline[JOB_ID]:
                                # Filtering on function within jobid
                                if (function_name and function_name in splitline[FUNC_SIG]) or \
                                    not function_name:
                                    profile_data.append(splitline)
                        # Filtering on job name (need not be perfect match)
                        elif jobname:
                            if jobname in splitline[JOB_NAME]:
                                # Filtering on function within job name
                                if (function_name and function_name in splitline[FUNC_SIG]) or \
                                    not function_name:
                                    profile_data.append(splitline)
                        # Filtering on function name across all jobs
                        elif function_name and function_name in splitline[FUNC_SIG]:
                            profile_data.append(splitline)
    else:
        # We only need to read the file which represents this task attempt
        if os.path.exists(os.path.join(stats_dir, taskid)):
            with (open(os.path.join(stats_dir, taskid))) as f:
                for line in f.readlines():
                    if line.startswith('Job Name, Job ID,'):
                        continue
                    # Space after ',' is important because that is how profile data is
                    # formatted. We split first so that we dont accidentally match jobab
                    # to jobabc in the string form.
                    splitline = line.split(', ')
                    # Filtering on function within jobid
                    if (function_name and function_name in splitline[FUNC_SIG]) or \
                        not function_name:
                        profile_data.append(splitline)
    return profile_data


def plotgraph(data):
    N = len(data)
    labels =[]
    aggr = []
    inv = []
    pct = []
    for key, value in data.items():
        labels.append(key)
        aggr.append(value[0])
        inv.append(value[1])
        pct.append(value[2])

    ind = range(N)  # the x locations for the groups
    height = 0.7       # the width of the bars
    plt.figure()
    plt.barh(ind, aggr, align='center', height=height, color=COLOR_AGGR)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Aggregate time in nanoseconds')

    plt.figure()
    plt.barh(ind, inv, align='center', height=height, color=COLOR_INV)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Number of invocations')

    plt.figure()
    plt.barh(ind, pct, align='center', height=height, color=COLOR_PCT)
    plt.yticks(ind, labels, horizontalalignment='left')
    plt.title('Per call time')
    plt.show()


def aggregateData(flags, raw_data):
    taskid = flags.taskid
    jobid = flags.job
    jobname = flags.jobname
    function_name = flags.function_name
    aggregated = {}
    if jobid or jobname or taskid:
        # We have either accumulated one (or all functions) from all task attempts
        # Combine these by function name
        d_aggr = defaultdict(int)
        d_inv = defaultdict(int)
        d_pct = defaultdict(float)
        for row in raw_data:
            d_aggr[row[FUNC_SIG]] += int(row[AGGREGATE_TIME])
            d_inv[row[FUNC_SIG]] += int(row[INVOCATIONS])
            d_pct[row[FUNC_SIG]] += float(row[PER_CALL_TIME])
        for key in d_aggr.keys():
            aggregated[key] = (d_aggr[key], d_inv[key], d_pct[key])
    elif function_name:
        # At this point, we are trying to view this function across task attempts
        d_aggr = defaultdict(int)
        d_inv = defaultdict(int)
        d_pct = defaultdict(float)
        for row in raw_data:
            d_aggr[row[TASK_ATTEMPT]] += int(row[AGGREGATE_TIME])
            d_inv[row[TASK_ATTEMPT]] += int(row[INVOCATIONS])
            d_pct[row[TASK_ATTEMPT]] += float(row[PER_CALL_TIME])
            for key in d_aggr.keys():
                aggregated[key] = (d_aggr[key], d_inv[key], d_pct[key])
    return aggregated


def displayData(flags, aggregated):
    if flags.to_file:
        with open(flags.to_file, 'w') as f:
            for key in aggregated:
                f.write(key + ', ' + ', '.join([str(x) for x in aggregated[key]]) + '\n')
    else:
        plotgraph(aggregated)


def main(args):
    FLAGS = BuildFlagParser().parse_args(args[1:])
    FLAGS.stats_dir = os.path.abspath(os.path.expanduser(FLAGS.stats_dir))
    if not (FLAGS.job or FLAGS.jobname or FLAGS.taskid or FLAGS.function_name):
        raise ValueError('Incorrect Arguments: You must specify either job id, job name, '
                         'task attempt or function for which you wish to collect stats.')
    raw_data = gatherData(FLAGS)
    aggregated = aggregateData(FLAGS, raw_data)
    displayData(FLAGS, aggregated)

if __name__ == '__main__':
    main(sys.argv)
