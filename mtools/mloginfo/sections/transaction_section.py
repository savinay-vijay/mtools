#SERVER-36414 - Log information about slow transactions

from collections import namedtuple
from operator import itemgetter

from .base_section import BaseSection
from mtools.util import OrderedDict
from mtools.util.grouping import Grouping
from mtools.util.print_table import print_table
import re

try:
    import numpy as np
except ImportError:
    np = None

LogTuple = namedtuple('LogTuple', ['lsid', 'txnNumber', 'autocommit',
                                   'readConcern','nscanned','numYield','timeActiveMicros','readTimestamp','terminationCause','locks','duration'])


def op_or_cmd(le):
    return le.operation if le.operation != 'command' else le.command


class TransactionSection(BaseSection):
    """TransactionSection class."""

    name = "transactions"

    def __init__(self, mloginfo):
        BaseSection.__init__(self, mloginfo)

        # add --transactions flag to argparser
        helptext = 'outputs statistics about transactions'
        self.mloginfo.argparser_sectiongroup.add_argument('--transactions',
                                                          action='store_true',
                                                          help=helptext)
        # add --tsort flag to argparser for transaction sort
        self.mloginfo.argparser_sectiongroup.add_argument('--tsort',
                                                          action='store',

                                                          choices=['duration'
                                                                ])

    @property
    def active(self):
        """Return boolean if this section is active."""
        return self.mloginfo.args['transactions']

    def run(self):

        """Run this section and print out information."""
        grouping = Grouping(group_by=lambda x: (x.lsid, x.txnNumber,
                                                x.autocommit, x.readConcern, x.nscanned,x.numYield,x.timeActiveMicros,x.readTimestamp,x.terminationCause,x.locks,x.duration))
        logfile = self.mloginfo.logfile

        if logfile.start and logfile.end:
            progress_start = self.mloginfo._datetime_to_epoch(logfile.start)
            progress_total = (self.mloginfo._datetime_to_epoch(logfile.end) -
                              progress_start)
        else:
            self.mloginfo.progress_bar_enabled = False

        for i, le in enumerate(logfile):
            # update progress bar every 1000 lines

            if self.mloginfo.progress_bar_enabled and (i % 1000 == 0):
                if le.datetime:
                    progress_curr = self.mloginfo._datetime_to_epoch(le
                                                                     .datetime)
                    if progress_total:
                        (self.mloginfo
                         .update_progress(float(progress_curr -
                                                progress_start) /
                                          progress_total))

            if (re.search('transaction', le.line_str)):


                lt = LogTuple(le.datetime, le.txnNumber, le.autocommit, le.readConcern, le.nscanned,le.numYields,le.timeActiveMicros,le.readTimestamp,le.terminationCause,le.locks,le.duration)
                grouping.add(lt)

        grouping.sort_by_size()

        # clear progress bar again
        if self.mloginfo.progress_bar_enabled:
            self.mloginfo.update_progress(1.0)

        # no queries in the log file
        if len(grouping) < 1:
            print('no transactions found.')
            return

        titles = ['lsid', 'txnNumber', 'autocommit', 'readConcern', 'keysExamined','numYield','timeActiveMicros','readTimestamp','terminationCause','locks','duration'
                  ]
        table_rows = []

        for g in grouping:
            # calculate statistics for this group
            lsid, txnNumber, autocommit, readConcern, keysExamined,numYield,timeActiveMicros,readTimestamp,terminationCause,locks,duration = g
            stats = OrderedDict()
            stats['lsid'] = lsid
            stats['txnNumber'] = txnNumber
            stats['autocommit'] = autocommit
            stats['readConcern'] = readConcern
            stats['keysExamined'] = keysExamined
            stats['numYield'] = numYield
            stats['timeActiveMicros'] = timeActiveMicros
            stats['readTimestamp'] = readTimestamp
            stats['terminationCause'] = terminationCause
            stats['locks'] = locks
            stats['duration'] = duration

            if stats['terminationCause'] == 'committed':
                le._commitedCount += 1

            elif stats['terminationCause'] == 'aborted':
                le._abortedCount += 1



            table_rows.append(stats)

        #if --tsort flag is set, sort transactions based on duration in descending order
        if(self.mloginfo.args['tsort'] == 'duration'):

                            table_rows = sorted(table_rows,
                            key=itemgetter(self.mloginfo.args['tsort']),
                            reverse=True)



        print_table(table_rows, titles, uppercase_headers=True)
        print('')

        #count total committed and total aborted transactions
        print("Total Committed: " + str(le._commitedCount))
        print("Total Aborted: " + str(le._abortedCount))
        print('')

        def logfile_generator(self):

            if len(self.mloginfo.args['logfile']) > 1:
                # merge log files by time
                for logevent in self._merge_logfiles():
                    yield logevent
            else:
                # only one file
                for logevent in self.mloginfo.args['logfile'][0]:
                    yield logevent

