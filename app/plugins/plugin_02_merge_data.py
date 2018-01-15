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
TICKTACK = 3.
# an offset shift by a some ticks
TICKTACK_OFFSET = 1.

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

# Unnesessary header
NAME = "02_merge_data"

##########
# Major work load - worker has 3 parameters to load, called as a thread
##########

class MergeDataWorker(PluginWorker):
    FILES2MERGE = []

    # maximum number of processes to spawn for individual task
    MAX_PROC = 5

    def work(self, *args, **kwargs):
        """ Do some useful work - lock/unlock functionality is already implemented"""
        PluginWorker.work(self, *args, **kwargs)

        # extract useful info
        self.raw_dir, self.temp_dir, self.proc_dir, self.output_dir, self.max_proc = self.form_var

        if not self.test(self.max_proc):
            self.max_proc = self.MAX_PROC

        # we do useful work only if the raw and the processed directories exits
        if self.check_directories(self.temp_dir, self.proc_dir):
            self.info("Directories exist ({})".format((self.temp_dir, self.proc_dir)))

            # obtain data to be processed
            self.get_existing_files()
        else:
            self.error("Could not find either temporary ({}) or processed dir ({}), please create them".format(self.temp_dir,
                                                                                                         self.proc_dir))

    def on_stop(self, *args, **kwargs):
        """
        Cleaning up the class parameters
        :param args:
        :param kwargs:
        :return:
        """
        # cleaning up
        if len(self.FILES2MERGE) > 0 and isinstance(self.FILES2MERGE, list):
            del self.FILES2MERGE[:]

        self.FILES2MERGE = []

        PluginWorker.on_stop(self, *args, **kwargs)

    def get_existing_files(self):
        """
        Processes files in the raw data, checks them for requirements
        :return:
        """
        # cleanup files which are useless from our point of view
        files2merge = glob.glob(os.path.join(self.temp_dir, "temp*"))
        self.check_existing_files(*files2merge)

        # process folders and data
        if len(self.FILES2MERGE) > 0:
            temp_files = copy.deepcopy(self.FILES2MERGE)

            # process folders and data
            self.process_raw_files(self.max_proc, *temp_files)

            # move processed files into the processed folder
            self.move_processed_files(self.max_proc, self.proc_dir, *temp_files)

    def check_existing_files(self, *args):
        """
        Check existing files for requirements - the folder should not be locked (.lock in the file name)
        :return:
        """
        temp = args

        files2merge = []

        if len(temp) > 0:
            # remove locked folders - skip folders with .lock and .dump in their names
            files2merge = filter(lambda p: not ".lock" in p and not ".dump" in p, temp)

        self.debug("List of folders containing files to process ({})".format(files2merge))
        if len(files2merge) > 0:
            self.FILES2MERGE = list(files2merge)

# default implementation of the exported work function
plugin_id = os.path.basename(__file__)
worker = MergeDataWorker(def_file=plugin_id, debug_level=PluginWorker.DEBUG)
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
