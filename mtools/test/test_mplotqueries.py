import sys
from datetime import datetime, timedelta
from random import randrange

from dateutil import parser
from nose.plugins.skip import SkipTest
from nose.tools import raises

import os

import mtools
from mtools.mplotqueries.mplotqueries import MPlotQueriesTool
from mtools.util.logevent import LogEvent
from mtools.util.logfile import LogFile


class TestMPlotQueries(object):

    def setup(self):
        """Startup method to create mloginfo tool."""
        self.tool = MPlotQueriesTool()
        self._test_init()

    def _test_init(self, filename='mongod_225.log'):
        # load logfile(s)
        self.logfile_path = "mtools/test/logfiles/mongod.log"
        # os.path.join(os.path.dirname(mtools.__file__),'test/logfiles/', filename)
        self.logfile = LogFile(open(self.logfile_path, 'rb'))


    def test_dns(self):
        self.tool.run('%s --dns --group connector' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))

    def test_oplog(self):
        self.tool.run('%s --oplog --group operation' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_queries(self):
        self.tool.run('%s --group operation' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_gap(self):
        self.tool.run('%s --type range --group operation --gap 600' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_rsstate(self):
        self.tool.run('%s --type rsstate' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_overlay(self):
        self.tool.run('%s --type scatter --overlay' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'overlay created:' in line, lines))


    def test_nscanned(self):
        self.tool.run('%s --type nscanned/n' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_yaxis(self):
        self.tool.run('%s --type scatter --yaxis w' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))


    def test_help(self):
        self.tool.run('%s -h' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'HELP' in line, lines))


    def test_overlay_list(self):
        self.tool.run('%s --type scatter --overlay --list' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'Existing overlays:' in line, lines))


    def test_overlay_reset(self):
        self.tool.run('%s --type scatter --overlay --reset' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'Deleted overlays.' in line, lines))


    def test_overlay_conchurn(self):
        self.tool.run('%s --logscale --group operation' % self.logfile_path)
        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'plot' in line, lines))

    def test_corrupt(self):
        # load different logfile
        logfile_path = os.path.join(os.path.dirname(mtools.__file__),
                                    'test/logfiles/', 'mongod_26_corrupt.log')
        self.tool.run('%s --queries' % logfile_path)

        output = sys.stdout.getvalue()
        lines = output.splitlines()
        assert any(map(lambda line: 'SCATTER plot' in line, lines))
        assert any(map(lambda line: line.startswith('id'), lines))

       

