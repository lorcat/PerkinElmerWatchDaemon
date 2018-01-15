__author__ = 'Konstantin Glazyrin'
"""
Each plugin file should be considered as a fully independent process.
This script should run as a stand alone and have implemented as much overhead troubleshooting as possible.
Please write it accordingly.
"""

# typical import for common funcitonality
from app.plugins.plugins_common import *
from app.workers import *

##########
# The header which must exist
##########

# tackt=2- in units of 2 base ticks (i.e. 0.2s for daemon basetick = 0.1s)
# the main thread will not allow tick tack cycles less than 1. and adjust accordingly
TICKTACK = 300.
# an offset shift by a some ticks
TICKTACK_OFFSET = 0.

def setup(obj):
    """
    Test for the correct initialization and parameter presence
    :param obj:
    :return:
    """
    global TICKTACK, TICKTACK_OFFSET
    if not obj.test(TICKTACK):
        raise NameError
    if not obj.test(TICKTACK_OFFSET):
        raise NameError


##########
# Major work load - worker has 3 parameters to load, called as a thread
##########

class TestWorker(PluginWorker):
    EXISTING_FILES = []

    FILES2REMOVE = []

    # maximum number of processes to spawn for individual task
    MAX_PROC = 5

    def work(self, *args, **kwargs):
        """ Do some useful work - lock/unlock functionality is already implemented"""
        PluginWorker.work(self, *args, **kwargs)

        # extract useful info
        self.raw_dir, self.temp_dir, self.proc_dir, self.output_dir, self.max_proc = self.form_var

        if not self.test(self.max_proc):
            self.max_proc = self.MAX_PROC

        # we do useful work only if the raw and the temporary directories exits
        if self.check_directories(self.raw_dir, self.temp_dir, self.proc_dir, self.output_dir):
            self.info("Directories exist ({})".format((self.raw_dir, self.temp_dir)))
        else:
            self.error("Could not find either some of the directories ({})".format((self.raw_dir, self.temp_dir, self.proc_dir, self.output_dir)))


# default implementation of the exported work function
plugin_id = os.path.basename(__file__)
worker = TestWorker(def_file=plugin_id, debug_level=PluginWorker.DEBUG)
work = worker.run

##########
# Major test functionality - plugin must be able to work as an independent script
# It saves time on testing
##########

if __name__ == "__main__":
    t = Tester(def_file="dumper", debug_level=Tester.DEBUG)
    timestamp = time.time()

    # initial parameters each plugin should have access to
    # directory storing all files with raw data
    # work_dir = "R:\\raw"
    work_dir = "D:\\tmp\\work\\raw"
    # directory storing files with temporary data
    # temp_dir = "R:\\temp"
    temp_dir = "D:\\tmp\\work\\tmp"
    # directory storing files with processed data
    # proc_dir = "R:\\processed"
    proc_dir = "D:\\tmp\\work\\processed"
    # directory storing files permanently
    # proc_dir = "R:\\output"
    output_dir = "D:\\tmp\\work\\output"

    # maximum number of processes to spawn
    max_proc = 5

    work(work_dir, temp_dir, proc_dir, output_dir, max_proc, unlock=True)

    t.info("Program was executed for ({}s)".format(time.time() - timestamp))
