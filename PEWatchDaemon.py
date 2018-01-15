__author__ = 'Konstantin Glazyrin'

import os
import sys

sys.path.append(os.path.dirname(__file__))

from app.common import *
from app.daemon import Daemon as MainWorker

from app.config import *

from PyTango import DeviceProxy, DevFailed, Device_4Impl, DeviceClass, DevState
from PyTango.server import Device, DeviceMeta, run, attribute, command

import threading

# main debug level
DEBUG_LEVEL = logging.INFO

class PeWatchDaemon(Device, Tester):
    __metaclass__ =  DeviceMeta

    # Attributes
    TickTackPeriod = attribute(label="Server Internal tick period, s", dtype=float, fget="getbase_tick_tack", unit="s")

    NumThreads = attribute(label="Server Total Thread number", dtype=int, fget="getbase_threads")

    # additional attributes
    DirRaw = attribute(doc="Local directory containing raw detector data", dtype=str,
                       fget="get_rawdir", fset="set_rawdir")
    DirTemp = attribute(doc="Local directory containing processed data (ready for merging and processing)", dtype=str,
                        fget="get_tempdir", fset="set_tempdir")
    DirProc = attribute(doc="Local directory containing finalized data (ready for transfer)", dtype=str,
                        fget="get_procdir", fset="set_procdir")
    DirOutput = attribute(doc="Remote directory under the OutputRoot (should be created automatically, in an iterative way)", dtype=str,
                          fget="get_outputdir", fset="set_outputdir")
    DirOutputRoot = attribute(doc="Remote root directory controlling the basic output direction (should exist)", dtype=str,
                           fget="get_outputroot", fset="set_outputroot")
    MaxProc = attribute(doc="Maximum number of threads for multithreading plugins (must be above 0)", dtype=int,
                        fget="get_maxproc", fset="set_maxproc")

    ONSTATE = DevState.ON
    FAULTSTATE = DevState.FAULT

    THREAD_DEMON = None

    logger = None

    def init_device(self):
        global DEBUG_LEVEL
        self.logger = Tester(def_file="BeamlineWatchDaemon", debug_level=DEBUG_LEVEL)
        self.logger.debug("Server is stopped")

        self.set_state(self.ONSTATE)

        #setup a worker
        self.worker = self.get_worker()
        self.thread = None

        self.Start()

    def delete_device(self):
        self.logger.debug("Server is stopped")
        self.Stop()

    @command()
    def Start(self):
        """
        Starts the daemon processing
        """
        self.logger.debug("Starting procedure")

        th = threading.Thread(target=self.worker.start)
        th.setDaemon(True)

        # saving a reference
        self.thread = th

        th.start()

        self.set_state(DevState.RUNNING)
        return DevState.RUNNING

    @command()
    def Stop(self):
        """
        Stops the daemon processing
        """
        self.logger.debug("Stopping procedure")

        if self.th is not None:
            self.worker.stop()

        threads = threading.enumerate()

        self.logger.debug("Worker thread ({})".format(self.thread))
        self.logger.debug("Threads on stop ({})".format(threads))

        for th in threads:
            main_thread = threading.currentThread()
            if th == main_thread:
                continue
            else:
                self.logger.debug("Joining {}".format(th.getName()))
                th.join(1)

        # cleanup th
        self.thread = None

        self.set_state(DevState.ON)
        return DevState.ON

    def getbase_tick_tack(self):
        return self.worker.TICKTACK / self.worker.MULTIPLIER

    def getbase_threads(self):
        return len(threading.enumerate())

    def get_worker(self):
        res = None
        try:
            if not self.worker:
                raise AttributeError
            else:
                res = self.worker
        except AttributeError:
            self.worker = MainWorker(debug_level=self.logger.debug_level)
            res = self.worker
        return res

    # getters
    def get_maxproc(self):
        """
        Returns the maxproc value from the worker
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()

        try:
            res = int(worker.maxproc)
        except ValueError:
            # fail safe
            res = CONFIG_INI_MAXPROC
            worker.maxproc = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    def get_rawdir(self):
        """
        Returns the rawdir value from the worker
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()

        try:
            res = str(worker.rawdir)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError
        except ValueError:
            # fail safe
            res = CONFIG_INI_RAW
            worker.rawdir = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    def get_tempdir(self):
        """
        Returns the tempdir value from the worker
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()

        try:
            res = str(worker.tempdir)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError
        except ValueError:
            # fail safe
            res = CONFIG_INI_TEMP
            worker.tempdir = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    def get_procdir(self):
        """
        Returns the procdir value from the worker
        :return:
        """

        self.logger.debug("Running ({})".format(sys._getframe(  ).f_code.co_name))

        worker = self.get_worker()

        try:
            res = str(worker.procdir)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError
        except ValueError:
            # fail safe
            res = CONFIG_INI_PROC
            worker.tempdir = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    def get_outputdir(self):
        """
        Returns the outputdir value from the worker
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()

        try:
            res = str(worker.outdir)
        except ValueError:
            # fail safe
            res = ""
            worker.outdir = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    def get_outputroot(self):
        """
        Returns the outputroot value from the worker
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()

        try:
            res = str(worker.outroot)
        except ValueError:
            # fail safe
            res = CONFIG_INI_OUTPUT_ROOT
            worker.outroot = res

            self.logger.error("Failsafe to default values ({})".format(res))

        return res

    # setters
    def set_maxproc(self, value):
        """
        Sets the maxproc value from the worker
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            value = int(value)
            if value > 0 and value < 10:
                worker.maxproc = value
            else:
                raise ValueError
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - limits (0:10)".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)

    def set_rawdir(self, value):
        """
        Sets the rawdir value from the worker
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            res = str(value)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError

            worker.rawdir = res
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - directory does not exist".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)

    def set_tempdir(self, value):
        """
        Sets the tempdir value from the worker
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            res = str(value)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError

            worker.tempdir = res
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - directory does not exist".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)

    def set_procdir(self, value):
        """
        Sets the tempdir value from the worker
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            res = str(value)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError

            worker.procdir = res
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - directory does not exist".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)

    def set_outputroot(self, value):
        """
        Sets the outputroot value from the worker
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            res = str(value)
            if not os.path.exists(res) or not os.path.isdir(res):
                raise ValueError

            worker.outroot = res
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - directory does not exist".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)

    def set_outputdir(self, value):
        """
        Sets the outputroot value from the worker - can be empty, but should avoid using special chars
        :param value:
        :return:
        """
        self.logger.debug("Running ({})".format(sys._getframe().f_code.co_name))
        worker = self.get_worker()
        self.logger.info("Setting the worker to the value ({}:{})".format(value, type(value)))

        try:
            value = re.sub("[!@#~*&^$\\\\./<>{}:;\"\'()]", '', str(value))

            worker.outdir = value
        except ValueError:
            # fail safe
            msg = "Wrong value was given ({} : {}) - directory does not exist".format(value, type(value))
            self.logger.error(msg)
            raise ValueError(msg)


if __name__ == "__main__":
    run([PeWatchDaemon])
