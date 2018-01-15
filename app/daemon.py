__author__ = 'Konstantin Glazyrin'

import os
import time

from copy import deepcopy
from functools import partial
from pluginbase import PluginBase
from app.config import *
from app.common import *
from app.common_keys import *


try:
    # python v2
    import ConfigParser as configparser
except ImportError:
    # python v3
    import configparser as configparser


import threading

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

plugin_base = PluginBase(package='app.daemon',
                         searchpath=[get_path('./plugins')])

class Daemon(Tester):
    ID = "daemon"

    TICKTACK = DAEMON_TICKTACK
    MAX_COUNTER = 10000.
    MULTIPLIER = DAEMON_MULTIPLIER

    BREAK = False

    PLUGIN_TEMPLATE = {NAME: None, TICKTACK: None, TICKTACK_OFFSET: None}

    # attribute equivalents
    RAW_DIR = ""
    TEMP_DIR = ""
    PROC_DIR = ""
    OUTPUT_DIR = ""
    OUTPUT_ROOT = ""
    MAXPROC = 5

    # error message if available
    ERRORMSG = ""

    @property
    def maxproc(self):
        self.debug("!!! Reading value {}".format(self.MAXPROC))
        return self.MAXPROC

    @maxproc.setter
    def maxproc(self, value):
        if value != self.MAXPROC:
            self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
            self.MAXPROC = value
            self.sync_ini_file(bsync=True)

    @property
    def rawdir(self):
        return self.RAW_DIR

    @rawdir.setter
    def rawdir(self, value):
        self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
        if value != self.RAW_DIR:
            self.RAW_DIR = value
            self.sync_ini_file(bsync=True)

    @property
    def tempdir(self):
        return self.TEMP_DIR

    @tempdir.setter
    def tempdir(self, value):
        self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
        if value != self.TEMP_DIR:
            self.TEMP_DIR = value
            self.sync_ini_file(bsync=True)

    @property
    def procdir(self):
        return self.PROC_DIR

    @procdir.setter
    def procdir(self, value):
        self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
        if value != self.PROC_DIR:
            self.PROC_DIR = value
            self.sync_ini_file(bsync=True)

    @property
    def outdir(self):
        return self.OUTPUT_DIR

    @outdir.setter
    def outdir(self, value):
        self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
        if value != self.OUTPUT_DIR:
            self.OUTPUT_DIR = value
            self.sync_ini_file(bsync=True)

    @property
    def outroot(self):
        return self.OUTPUT_ROOT

    @outroot.setter
    def outroot(self, value):
        self.debug("(*) Setting the ({}) to ({})".format(sys._getframe().f_code.co_name, value))
        if value != self.OUTPUT_ROOT:
            self.OUTPUT_ROOT = value
            self.sync_ini_file(bsync=True)

    def __init__(self, debug_level=None):
        """
        Initializes the class, scans the plugins and reloads them if necessary
        :param debug_level:
        """
        Tester.__init__(self, def_file=self.ID, debug_level=debug_level)

        # load configuration file
        self.load_ini_variables()

        # plugins
        global plugin_base
        self.plugin_base = plugin_base.make_plugin_source(
            identifier=self.ID,
            searchpath=[get_path('./plugins')])

        self.plugins = []

        self.plugin_info = []

        for plugin_name in self.plugin_base.list_plugins():
            self.debug("Found a plugin with name ({})".format(plugin_name))

            plugin = self.plugin_base.load_plugin(plugin_name)
            try:
                # simple test of the plugin validity - raises NameError or Attribute error if the plugin is not valid
                plugin.setup(self)

                # copy information on the plugin to the general storage
                tmpl = deepcopy(self.PLUGIN_TEMPLATE)
                tmpl[NAME] = plugin_name
                tmpl[TICKTACK] = plugin.TICKTACK
                tmpl[TICKTACK_OFFSET] = plugin.TICKTACK_OFFSET
                self.plugin_info.append(tmpl)

                test = plugin.work

                self.debug("Plugin ({}) is valid, adding it".format(plugin_name))
                self.plugins.append(plugin)
                self.debug("Plugin ({}) has a tact of ({})".format(plugin_name, plugin.TICKTACK))

                # test plugin tact
                if plugin.TICKTACK < self.TICKTACK:
                    self.error("Plugin ({}) has low TICKTACK value ({}), matching it with default ({})".format(plugin_name, plugin.TICKTACK, self.TICKTACK))
                    plugin.TICKTACK = self.TICKTACK
            except (NameError, AttributeError):
                self.error("Plugin ({}) is invalid".format(plugin_name))

        # internal counter
        self.counter = 1
        self.remove_locks()

    def load_ini_variables(self):
        """
        Load variables form ini file
        :return:
        """
        self.debug("Loading configuration from an ini file ({})".format(CONFIG_INI))
        parser = configparser.RawConfigParser(allow_no_value=True)

        # one could think about test for directory, but who cares
        self.sync_ini_file()

        # assuming that the file exist
        parser.read(config.CONFIG_INI)

        # setting the
        keys = (CFG_MAXPROC, CFG_OUTDIR, CFG_OUTROOT, CFG_RAWDIR, CFG_TEMPDIR, CFG_PROCDIR)
        bsync = False
        for key in keys:
            try:
                value = parser.get(config.CFG_SECTION, key)

                # self.debug("Assigning ini values ({}/{})".format(key, value))

                if key == CFG_MAXPROC:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.MAXPROC = value
                elif key == CFG_OUTDIR:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.OUTPUT_DIR = value
                elif key == CFG_OUTROOT:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.OUTPUT_ROOT = value
                elif key == CFG_RAWDIR:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.RAW_DIR = value
                elif key == CFG_TEMPDIR:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.TEMP_DIR = value
                elif key == CFG_PROCDIR:
                    self.debug("(+) Setting the ({}) to ({}/{})".format(sys._getframe().f_code.co_name, key, value))
                    self.PROC_DIR = value

                self.debug("Found an ini file value ({}/{})".format(key, value))
            except configparser.NoOptionError:
                bsync = True

                value = ""
                if key == CFG_MAXPROC:
                    value = CONFIG_INI_MAXPROC
                elif key == CFG_OUTDIR:
                    value = ""
                elif key == CFG_OUTROOT:
                    value = CONFIG_INI_OUTPUT_ROOT
                elif key == CFG_RAWDIR:
                    value = CONFIG_INI_RAW
                elif key == CFG_TEMPDIR:
                    value = CONFIG_INI_TEMP
                elif key == CFG_PROCDIR:
                    value = CONFIG_INI_PROC

                self.warning("Adding a missing value ({}/{})".format(key, value))
                parser.set(CFG_SECTION, key, value)

        if bsync:
            self.sync_ini_file(bsync=bsync)

        self.debug("Proc test 01 ({})".format(self.PROC_DIR))

    def sync_ini_file(self, bsync=False):
        """
        Tests if the ini file exist or not and creates one if needed
        :return:
        """
        bsave = True
        parser = configparser.RawConfigParser()

        value_dict = None

        if not os.path.exists(config.CONFIG_INI):
            value_dict = {
                CFG_MAXPROC: CONFIG_INI_MAXPROC,
                CFG_OUTDIR: "",
                CFG_OUTROOT: CONFIG_INI_OUTPUT_ROOT,
                CFG_RAWDIR: CONFIG_INI_RAW,
                CFG_TEMPDIR: CONFIG_INI_TEMP,
                CFG_PROCDIR: CONFIG_INI_PROC
            }
        elif bsync:
            value_dict = {
                CFG_MAXPROC: self.MAXPROC,
                CFG_OUTDIR: self.OUTPUT_DIR,
                CFG_OUTROOT: self.OUTPUT_ROOT,
                CFG_RAWDIR: self.RAW_DIR,
                CFG_TEMPDIR: self.TEMP_DIR,
                CFG_PROCDIR: self.PROC_DIR
            }
        else:
            bsave = False

        if bsave:
            self.debug("Saving the configuration file ({})".format(CONFIG_INI))
            parser.add_section(CFG_SECTION)

            for key in sorted(value_dict.keys()):
                parser.set(CFG_SECTION, key, value_dict[key])

            with open(CONFIG_INI, 'wb') as configfile:
                parser.write(configfile)

        self.debug("Proc test 02 ({})".format(self.PROC_DIR))


    def start(self):
        """
        Main procedure - performs a while loop with tact matching
        :return:
        """
        self.BREAK = False
        while not self.BREAK:
            if len(self.plugins) == 0:
                self.error("No plugins found, exiting")
                break
            else:
                self.debug("Found these plugins ({})".format(self.plugins))

            for plugin in self.plugins:
                tact = int(plugin.TICKTACK)
                base_tact = int(self.counter) - plugin.TICKTACK_OFFSET

                # start new thread if a proper tact has come
                if base_tact > 0 and not base_tact % tact:
                    # self.info("!!! plugin {} tact {} basetact {} function {}".format(plugin, tact, base_tact, base_tact % tact))
                    self.debug("Firing a tact ({}:{}:{})".format(self.counter, self.TICKTACK, self.counter * self.TICKTACK))
                    self.debug("Running a plugin ({}); main tact ({}) plugin tact ({}); time ({})".format(plugin, base_tact, tact, time.time()))
                    self.start_thread(plugin)

            # sleep for a tact
            sleep_time = float(self.TICKTACK) / float(self.MULTIPLIER)
            self.debug("Sleeping a tact ({}:{})".format(self.counter, sleep_time))
            time.sleep(sleep_time)
            self.counter = self.counter + 1

            # reset counter in order to avoid overflow
            if self.counter == self.MAX_COUNTER:
                if not self.MAX_COUNTER % 2:
                    self.counter = 2
                else:
                    self.counter = 1

    def start_thread(self, plugin):
        """
        Start a thread with a specific plugin
        :param plugin:
        :return:
        """
        self.debug("Starting a plugin thread ({})".format(plugin))

        # work_dir = "R:\\raw"
        raw_dir = self.RAW_DIR
        # directory storing files with temporary data
        # temp_dir = "R:\\temp"
        temp_dir = self.TEMP_DIR
        # directory storing files with processed data
        # proc_dir = "R:\\processed"
        proc_dir = self.PROC_DIR
        # directory storing files permanently
        # outdir_dir = "R:\\output"
        output_dir = os.path.join(self.OUTPUT_ROOT, self.OUTPUT_DIR)

        try:
            os.makedirs(output_dir)
        except OSError as exc:
            if os.path.exists(output_dir) and os.path.isdir(output_dir):
                self.debug("Directory ({}) exists".format(output_dir))
            else:
                output_dir = self.OUTPUT_ROOT
                msg = "Could not create a directory ({})".format(output_dir)
                self.error(msg)

        try:
            max_proc = int(self.MAXPROC)
        except ValueError:
            max_proc = CONFIG_INI_MAXPROC

        name = "Thread"
        try:
            name = plugin.NAME
        except AttributeError:
            pass

        th = threading.Thread(target=plugin.work, name=name, args=(raw_dir, temp_dir, proc_dir, output_dir, max_proc))
        th.start()

    def remove_locks(self):
        """
        Removes old lock files on the startup
        :return:
        """
        m = MutexLock(def_file="lock_remover")
        m.unlock_all()

    def stop(self):
        """
        Sets the parameter and quits the thread
        :return:
        """
        self.debug("Received an exit message, quiting")
        self.BREAK = True
        time.sleep(0.1)

    def get_plugin_info(self):
        """
        Returns information on the 'good' - loaded plugins in the form of text
        :return:
        """
        res = ""
        if len(self.plugin_info) > 0:
            for (i, el) in enumerate(self.plugin_info):
                res += "{:20s}\t{}\t{}\n".format(el[NAME], el[TICKTACK], el[TICKTACK_OFFSET])
        else:
            res = "No plugin has been found"
        return res

# TODO: Fix ticktack + offset issue of the plugins