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

# Unnesessary header
NAME = "01_prepare_raw"


##########
# Major work load - worker has 3 parameters to load, called as a thread
##########

class RawDataWorker(PluginWorker):
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
        if self.check_directories(self.raw_dir, self.temp_dir):
            self.info("Directories exist ({})".format((self.raw_dir, self.temp_dir)))

            # filter raw data, find files which satisfy requirements
            self.get_existing_files()
        else:
            self.error("Could not find either raw ({}) or temporary dir ({}), please create them".format(self.raw_dir,
                                                                                       self.temp_dir))

    def on_stop(self, *args, **kwargs):
        """
        Cleaning up the class parameters
        :param args:
        :param kwargs:
        :return:
        """
        # cleaning up
        if len(self.EXISTING_FILES) > 0 and isinstance(self.EXISTING_FILES, list):
            del self.EXISTING_FILES[:]

        if len(self.FILES2REMOVE) > 0 and isinstance(self.FILES2REMOVE, list):
            del self.FILES2REMOVE[:]

        self.EXISTING_FILES = []
        self.FILES2REMOVE = []

        PluginWorker.on_stop(self, *args, **kwargs)

    def get_existing_files(self):
        """
        Processes files in the raw data, checks them for requirements
        :return:
        """
        # cleanup files which are useless from our point of view
        self.FILES2REMOVE = list(glob.glob(os.path.join(self.raw_dir, "*dark*")))

        self.debug("List of files to remove ({})".format(self.FILES2REMOVE))
        if len(self.FILES2REMOVE) > 0:
            self.remove_bad_files()

        # find files which are useful
        files2store = glob.glob(os.path.join(self.raw_dir, "*.tif"))
        self.check_existing_files(*files2store)

        # move useful files to the new directories with lock
        self.move_existing_files()

    def remove_bad_files(self):
        """
        Removes the files found to be unnecessary
        :return:
        """
        # TODO: remove files by spanning some processes
        for fn in self.FILES2REMOVE:
            self.debug("Removing ({})".format(fn))

        temp_copy = copy.deepcopy(self.FILES2REMOVE)
        self.remove_raw_files(self.max_proc, *temp_copy)

    def check_existing_files(self, *args):
        """
        Check existing files for requirements - metadata should exist together with tif file.
        Tif and metadata should have non zero size.
        Modification time for tif and metadata should deviate by a DELAY parameter with respect to the current date.
        :return:
        """
        temp = args
        files2raw = []

        # remove darks
        patt = re.compile("^((?!dark).)*\.tif")
        files2raw = []
        for file in temp:
            if patt.match(file):
                files2raw.append(file)

        self.debug("List of promising files ({})".format(files2raw))
        if len(files2raw) > 0:
            self.EXISTING_FILES = list(self.check_raw_files(*files2raw))

    def move_existing_files(self):
        """
        Moves the existing files to the temporary folder
        :return:
        """
        if len(self.EXISTING_FILES) > 0:
            temp_files = copy.deepcopy(self.EXISTING_FILES)
            self.debug("Starting the file moving process ({})".format(temp_files))
            self.move_raw_files(self.max_proc, self.temp_dir, *temp_files)


# default implementation of the exported work function
plugin_id = os.path.basename(__file__)
worker = RawDataWorker(def_file=plugin_id, debug_level=PluginWorker.DEBUG)
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

# TODO: optional splitting of the plugin - darks are cleaned separately, raw files are treated separately.