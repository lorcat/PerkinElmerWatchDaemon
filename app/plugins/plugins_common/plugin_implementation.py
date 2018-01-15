__author__ = 'Konstantin Glazyrin'

"""
General plugin object oriented implementation
"""

import shutil
import tempfile
import queue
import threading
import re

# data format specific
import fabio
import h5py
import copy

from app.common import *

KEY_UNLOCK = "unlock"

# timeout in s to wait upon os operation
OSTIMEOUT = 1
# sleep time inbetween waiting for OS operation
OSSLEEP = 0.1

class PluginWorker(MutexLock):
    # value controlling check for test for a delay after the last file modification (s)
    FILE_MODIFICATION_DELAY = 0.2

    # value controlling minimal size of the file for the test of a valid file
    FILE_SIZE_THRESHOLD = 8

    def __init__(self, def_file=None, debug_level=None):
        MutexLock.__init__(self, def_file=def_file, debug_level=debug_level)

        self.id = def_file

    def run(self, *args, **kwargs):
        """
        General macro implementing a functionality
        :return:
        """
        self.debug("Entering the abstract implementation of run() function")
        self.debug("Formal arguments are ({})".format(args))
        self.debug("Variable length arguments are ({})".format(kwargs))

        # functionality on start
        res = self.on_start(args, kwargs)

        # useful load
        if self.test(res):
            self.work(args, kwargs)

        # functionality on stop
        self.on_stop(args, kwargs)

    def on_start(self, *args, **kwargs):
        """
        Abstarct implementation of the on_start function
        :return: (bool) - state of the lock - locked or not
        """
        self.debug("Entering the abstract implementation of on_start()")
        self.debug("Input parameters are args ({}) and kwargs ({})".format(*args, **kwargs))
        self.form_var, self.var_var = args[0], args[1]

        res = True

        # test if we need to force unlock
        if self.var_var.has_key(KEY_UNLOCK):
            if self.var_var[KEY_UNLOCK]:
                self.debug("Unlocking by force")
                self.unlock()

        if self.is_locked():
            self.debug("Device is locked.. Aborting.")
            res = False

        self.lock()

        return res

    def on_stop(self, *args, **kwargs):
        """
        Abstarct implementation of the on_stop function
        :return:
        """
        self.debug("Entering the abstract implementation of on_stop()")
        self.debug("Input parameters are args ({}) and kwargs ({})".format(*args, **kwargs))
        form_var, var_var = args[0], args[1]

        # unlocking on stop
        self.unlock()

    def work(self, *args, **kwargs):
        """
        Abstarct implementation of the main load function
        :return:
        """
        self.debug("Entering the abstract implementation of work()")
        self.debug("Input parameters are args ({}) and kwargs ({})".format(*args, **kwargs))
        form_var, var_var = args[0], args[1]

    def check_directories(self, *args):
        """
        Tests that the provided directories exist
        :param args:
        :return:
        """
        res = None
        if len(args) > 0:
            res = True
            for el in args:
                if not os.path.isdir(el):
                    self.error("Directory ({}) does not exist".format(el))
                    res = False
                    break
        return res

    def check_raw_files(self, *args, **kwargs):
        """
        Checks non zero file size requirements for the files
        :return:
        """
        res = []

        self.debug("List of files is ({})".format(args))
        for fn in args:
            self.debug("Checking file ({}) for requirements".format(fn))

            # get timestamp
            timestamp = time.time()

            fnmeta = self.get_meta(fn)

            # check that files exist
            if not os.path.exists(fn) or not os.path.exists(fnmeta):
                self.warning("Either the ({}) or ({}) do not exist".format(fn, fnmeta))
                continue

            try:
                # check if files have proper timestamp of modification time
                fm, fmmeta = os.path.getmtime(fn), os.path.getmtime(fnmeta)

                if timestamp - fm < self.FILE_MODIFICATION_DELAY or timestamp - fmmeta < self.FILE_MODIFICATION_DELAY:
                    self.warning("Files ({}) or ({}) did not pass the time of modification test")
                    raise ValueError

                # check size
                fs, fsmeta = os.path.getsize(fn), os.path.getsize(fnmeta)

                if fs < self.FILE_SIZE_THRESHOLD or fsmeta < self.FILE_SIZE_THRESHOLD:
                    self.warning("Files ({}) or ({}) did not pass the time of modification test")
                    raise ValueError
            except OSError as e:
                self.error("Either the main file ({}) or its meta ({}) have troubles".format(fn, fnmeta))
                continue
            except ValueError:
                continue

            # seems like the file is good - add it to the existing files
            self.debug("Marking the file ({}) and its meta ({}) as valid".format(fn, fnmeta))
            res.append(fn)
        return res

    def get_meta(self, filename):
        """
        Returns name for the meta file
        :param filename:
        :return:
        """
        return "{}{}".format(filename, ".metadata")

    def move_raw_files(self, max_proc, outdir, *args):
        """
        Copies files to the directory, unlocks directory - renames to the value without .lock
        Thread based, limited by the maximum thread count of max_proc
        :return:
        """

        q = queue.Queue()
        if os.path.isdir(outdir):
            for fn in args:
                fnmeta = self.get_meta(fn)

                # check that files exist
                if not os.path.exists(fn) or not os.path.exists(fnmeta):
                    self.warning("Either the ({}) or ({}) do not exist".format(fn, fnmeta))
                    continue

                # add to a queue
                q.put((fn, fnmeta, outdir))

        threads = []
        for i in range(max_proc):
            th = threading.Thread(target=_move_raw_file, args=(q,), name="move_raw")
            threads.append(th)
            th.start()

        # block until the work is done
        q.join()

        # join available threads
        for th in threads:
            th.join()

        self.debug("Moving raw files procedure is finished")

    def remove_raw_files(self, max_proc, *args):
        """
        Spans multiprocessing (thread), removes files and waits until the multiprocess ends its operation
        Python module multiprocessing was extremely slow for IO operation with the drive. We are limiting ourselves by max_proc argument
        :param max_proc:
        :param args:
        :return:
        """

        timestamp = time.time()
        threads = []

        q = queue.Queue()
        for (i, fn) in enumerate(args):
            q.put(fn)

        for i in range(max_proc):
            th = threading.Thread(target=_remove_file, args=(q,), name="remove_raw")
            th.start()
            threads.append(th)

        q.join()
        for th in threads:
            th.join()

        self.debug("Pool was working for ({}s)".format(time.time() - timestamp))

    def process_raw_files(self, max_proc, *args):
        """
        Spans multiprocessing (thread), locks the temporary directory, parses metadata,
        Python module multiprocessing was extremely slow for IO operation with the drive. We are limiting ourselves by max_proc argument
        :param max_proc:
        :param args:
        :return:
        """

        timestamp = time.time()
        threads = []

        q = queue.Queue()
        for (i, fn) in enumerate(args):
            q.put(fn)

        for i in range(max_proc):
            th = threading.Thread(target=_merge_tiff_data, args=(q,), name="process_raw")
            th.start()
            threads.append(th)

        q.join()
        for th in threads:
            th.join()

        self.debug("Pool was working for ({}s)".format(time.time() - timestamp))

    def move_processed_files(self, max_proc, outdir, *args):
        """
        Copies files to the directory, unlocks directory - renames to the value without .lock
        Thread based, limited by the maximum thread count of max_proc
        :return:
        """

        q = queue.Queue()
        if os.path.isdir(outdir):
            for path in args:
                # add to a queue
                lock_path = "{}{}".format(path, '.lock')

                # if we could lock - add to queue
                if _shmove(path, lock_path, self):
                    q.put(lock_path)

        threads = []
        for i in range(max_proc):
            th = threading.Thread(target=_move_processed_file, args=(q, outdir,), name="move_processed")
            threads.append(th)
            th.start()

        # block until the work is done
        q.join()

        # join available threads
        for th in threads:
            th.join()

        self.debug("Moving processed files procedure is finished")

    def finalize_files(self, max_proc, outdir, *args):
        """
        Copies files to the directory, unlocks directory - renames to the value without .lock
        Thread based, limited by the maximum thread count of max_proc
        :return:
        """
        q = queue.Queue()
        if os.path.isdir(outdir):
            for path in args:
                # add to a queue
                lock_path = "{}{}".format(path, '.lock')

                # if we could lock - add to queue
                if _shmove(path, lock_path, self):
                    q.put(lock_path)

        threads = []
        for i in range(max_proc):
            th = threading.Thread(target=_move_finalized_files, args=(q, outdir,), name="finalize_files")
            threads.append(th)
            th.start()

        # block until the work is done
        q.join()

        # join available threads
        for th in threads:
            th.join()

        self.debug("Finalization procedure of  is finished, files were copied to the remote directory ({})".format(outdir))

###
# individual worker functions - as less memory consumption as possible
###

def _move_raw_file(local_queue, t=None):
    """
    Simple command to move raw files into a temporary folder
    :param local_queue
    :return:
    """
    t = _get_tester(t)

    while not local_queue.empty():
        item = local_queue.get()

        fn, fnmeta, outdir = item

        # create a temporary folder
        tempfolder = tempfile.mkdtemp(suffix='.lock', prefix='temp_', dir=outdir)
        finalfolder = tempfolder.replace(".lock", "")

        t.debug("Copying file ({}) and its meta ({}) to a new folder ({})".format(fn, fnmeta, tempfolder))

        # make sure all the files are not readonly
        for p in (fn, fnmeta, tempfolder):
            os.chmod(p, stat.S_IWRITE)

        # move files to this directory
        for attempt in range(5):
            try:
                shutil.move(fn, tempfolder)
                shutil.move(fnmeta, tempfolder)
                break
            except (OSError, IOError) as e:
                t.error("OSError or IOError has occurred, we may have been too fast with renaming - try again..\n{} : {} : {}".format(
                        e.errno, e.message, e.strerror))
                time.sleep(0.1)
                continue

        # unlock
        _shmove(tempfolder, finalfolder, t)

        local_queue.task_done()

def _move_processed_file(local_queue, outdir, t=None):
    """
    Simple command to move raw files into a temporary folder
    :param local_queue
    :return:
    """
    t = _get_tester(t)

    while not local_queue.empty():
        item = local_queue.get()

        path = item

        # create a temporary folder
        newpath = os.path.join(outdir, os.path.basename(path))
        finalpath = os.path.join(newpath.replace(".lock", ""))

        t.debug("Moving processed data ({}) to a new folder ({})".format(path, outdir))
        t.debug("Renaming processed data ({}) to a new folder ({})".format(newpath, finalpath))


        # make sure all the files are not readonly
        os.chmod(path, stat.S_IWRITE)

        # move files into this directory
        shutil.move(path, outdir)

        # unlock
        _shmove(newpath, finalpath, t)

        # stop if there were too many errors
        local_queue.task_done()

def _move_finalized_files(local_queue, outdir, t=None):
    """
    Simple command to move raw files into a temporary folder
    :param local_queue
    :return:
    """
    t = _get_tester(t)

    t.debug("Trying to finalize files into ({})".format(outdir))

    while not local_queue.empty():
        item = local_queue.get()

        # path containing all the files
        path = item
        t.debug("Origin directory: ({})".format(path))

        # files in the directory
        files = glob.glob(os.path.join(path, "*"))
        t.debug("List of files to move: ({})".format(files))

        if len(files) > 0:
            for file in files:
                t.debug("Moving file ({}) to a new folder ({})".format(file, outdir))

                # move files to this directory
                _shcopy(file, outdir, t)

        # remove the path
        _shrmtree(path, t)

        local_queue.task_done()

def _remove_file(local_queue, t=None):
    """
    Simple command to remove individual files, based on the queue
    :param local_queue:
    :return:
    """
    t = _get_tester(t)

    while not local_queue.empty():
        path = local_queue.get()


        t.info("Removing a system object ({})".format(path))

        # removing the path
        _shrmtree(path, t)

        local_queue.task_done()


def _on_shutilerror(func, path, exc_info):
    """
    Changes the
    :param func:
    :param path:
    :param exc_info:
    :return:
    """
    # TODO: fix OS + Windows Error
    os.chmod(path, stat.S_IWRITE)
    os.unlink(path)


def _merge_tiff_data(local_queue, t=None):
    """
    Merges local data
    :param local_queue:
    :return:
    """
    t = _get_tester(t)

    while not local_queue.empty():
        path = local_queue.get()

        t.debug("Processing task {}".format(path))

        # path should exist and contain some tif file and its meta - one file - one meta
        if os.path.exists(path):
            ref_path = os.path.join(path, "*.tif")

            t.debug("Using reference file path {}".format(ref_path))

            files2merge = glob.glob(ref_path)
            if len(files2merge) > 0:
                for fn in files2merge:
                    # test for meta file
                    fnmeta = "{}.metadata".format(fn)
                    t.debug("{}/{}".format(fn, fnmeta))

                    if os.path.exists(fn) and os.path.exists(fnmeta):
                        # do the work - read meta, merge with tif
                        header = _single_file_merge(fn, fnmeta, t=t)

                        # do the work - create NXS file and merge
                        # TODO: create NXS file with references
                        _make_nexus_from_tif(fn, fnmeta, header, t=t)

        else:
            pass
        local_queue.task_done()

def _single_file_merge(fn, fnmeta, t=None):
    """
    Reads data from meta, adds the header to the TIF
    :return:
    """
    t = _get_tester(t)

    t.debug("Merging files ({}/{})".format(fn, fnmeta))

    header = {}
    max_lines = 30

    header = {}

    try:
        bflag = True
        counter = 0
        patt = re.compile('^\s*(dateString|userComment[0-9]|exposureTime|summedExposures)=(.*)$')



        fh = open(fnmeta, "r")
        for line in fh:
            line = line.strip()

            m = patt.match(line)
            if t.test(m):
                h, v = m.groups()
                t.debug("Match found ({}/{})".format(h, v))
                header[h] = v

            # break to avoid unnecessary file reading
            counter += 1
            if counter > max_lines:
                break

        t.debug("The metadata header is ({})".format(header))

        # setting the header - open file, set the header, update
        img = fabio.open(fn)
        img.update_header(**header)
        img.save(fn)
    except IOError:
        t.error("Could not access the meta file")

    return header

# set up default element
NXKEYROOT, NXKEYDATA, NXKEYDEFAULT, NXKEYDETECTOR, NXKEYINSTRUMENT, NXKEYHEADER = 'root', 'data', 'default', 'detector', 'instrument', 'header'
NXENTRY, NXCLASS, NXDATA = 'NXentry', 'NX_class', 'NXdata'
NXDETECTOR, NXINSTRUMENT = 'NXdetector', 'NXinstrument'

def _make_nexus_from_tif(fn, fnmeta, header, t=None):
    """
    Create the final nexus file with a tree
    :return:
    """
    t = _get_tester(t)

    base_dir = os.path.dirname(fn)
    base_name = os.path.splitext(os.path.basename(fn))[0]

    nxs_name = os.path.join(base_dir, u'{}{}'.format(base_name, u'.nxs'))

    nxfh = h5py.File(nxs_name, "w")
    nxfh.attrs['default'] = NXKEYROOT

    # here one can implement an additional merge of the as prepared configurationin formation
    # and merge information from external sources, such as memcached
    # TODO: implement memcache lookup and merge

    nxdict = {'instrument': {'name': "P02.2 beamline of Petra-III",
                             'name@shortname': "P02.2 beamline of Petra-III",
                             NXKEYDETECTOR: {'name': 'PE XRD1621',
                                             'header': header},
                             },
              'data': {'source_attr': fn, 'raw_path': os.path.basename(fn), 'meta_path': os.path.basename(fnmeta)}}

    # recursively build a nexus tree - take into account the paths, attributes and values
    _nxs_create_child_group(nxfh, child_name=NXKEYROOT, child_class=NXENTRY, default=NXKEYDATA, data=nxdict)

    nxfh.close()

def _nxs_create_child_group(nxroot, child_name, child_class, default=None, data=None):
    """
    Creates a new
    :param root:
    :param child_name:
    :param child_type:
    :param default:
    :return:
    """
    nxgroup = nxroot.create_group(child_name)
    nxgroup.attrs[NXCLASS] = child_class

    if default is not None:
        nxgroup.attrs[NXKEYDEFAULT] = default

    # todo - split dataset by keys and external links
    if isinstance(data, dict):
        for key in data.keys():
            local_data = data[key]
            if not isinstance(local_data, dict):
                # create data out of lists
                if isinstance(local_data, list) or isinstance(local_data, tuple):
                    data_set = nxgroup.create_dataset(key, data=local_data)

                # look for data which will be set as an attribute and which will be set as value
                needle_attr, needle_path = "_attr", "_path"
                if needle_attr in key:
                    nxgroup.attrs[key.replace(needle_attr, "")] = data[key]
                else:
                    nxgroup.create_dataset(key, data=local_data)
            elif key is not None:
                # build a recursive tree
                _nxs_create_child_group(nxgroup, key, "NX{}".format(key.lower()), data=data[key])
    return nxgroup

def _get_tester(t=None):
    """
    Wrapper assigning the same log file to the tester
    :param t:
    :return:
    """
    res = t
    if t is None:
        res = Tester(nofile=True)
    return res

def _shmove(source, dest, logger, timeout=1):
    """
    Completes the system move function - waits for successful attempt or timeout
    :param source:
    :param dest:
    :param logger:
    :return:
    """
    bsuccess = False

    if timeout is None:
        timeout = OSTIMEOUT

    start_time = time.time()

    try:
        if not os.path.exists(source):
            raise IOError

        while not bsuccess:
            try:
                # TODO: fix OS + Windows Error
                os.chmod(source, stat.S_IWRITE)
                shutil.move(source, dest)

                # copy has succeed
                bsuccess = True

                raise ValueError
            except (OSError, IOError) as e:
                logger.error("OsError while moving ({}) to  ({}):\n{} : {}".format(source, dest, e.errno,
                                                                                   e.message))
                time.sleep(OSSLEEP)

            # break on timeout
            if time.time() - start_time > timeout:
                raise AttributeError
    except ValueError:
        logger.debug("Source ({}) was successfuly moved to ({})".format(source, dest))
    except AttributeError:
        logger.error("Timeout while moving ({}) to  ({})".format(source, dest))
    except IOError:
        logger.error("Source does not exist".format(source))
    return bsuccess

def _shcopy(source, dest, logger, timeout=None):
    """
    Completes the system move function - waits for successful attempt or timeout
    :param source:
    :param dest:
    :param logger:
    :return:
    """
    bsuccess = False

    if timeout is None:
        timeout = OSTIMEOUT

    start_time = time.time()

    try:
        if not os.path.exists(source):
            raise IOError

        while not bsuccess:
            try:
                # TODO: fix OS + Windows Error
                os.chmod(source, stat.S_IWRITE)
                shutil.copy(source, dest)

                # copy has succeed
                bsuccess = True

                raise ValueError
            except (OSError, IOError) as e:
                logger.error("OsError while copying ({}) to  ({}):\n{} : {}".format(source, dest, e.errno,
                                                                                   e.message))
                time.sleep(OSSLEEP)

            # break on timeout
            if time.time() - start_time > timeout:
                raise AttributeError
    except ValueError:
        logger.debug("Source ({}) was successfuly copied to ({})".format(source, dest))
    except AttributeError:
        logger.error("Timeout while copying ({}) to  ({})".format(source, dest))
    except IOError:
        logger.error("Source does not exist".format(source))
    return bsuccess

## Functions for working with utilities

def _shrmtree(source, logger, timeout=None):
    """
    Completes the system move function - waits for successful attempt or timeout
    :param source:
    :param dest:
    :param logger:
    :return:
    """
    bsuccess = False

    if timeout is None:
        timeout = OSTIMEOUT

    start_time = time.time()



    try:
        if not os.path.exists(source):
            raise IOError
        while not bsuccess:
            try:
                # TODO: fix OS + Windows Error
                os.chmod(source, stat.S_IWRITE)
                shutil.rmtree(source, onerror=_on_shutilerror)

                # copy has succeed
                bsuccess = True

                raise ValueError
            except (OSError, IOError) as e:
                logger.error("OsError while deleting ({}) :\n{} : {}".format(source, e.errno,
                                                                                   e.message))
                time.sleep(OSSLEEP)

            # break on timeout
            if time.time() - start_time > timeout:
                raise AttributeError
    except ValueError:
        logger.debug("Source ({}) was successfuly deleted".format(source))
    except AttributeError:
        logger.error("Timeout while deleting ({})".format(source))
    except IOError:
        logger.error("Source does not exist".format(source))
    return bsuccess

# test
if __name__ == '__main__':
    worker = PluginWorker(__file__)
    worker.run("/raw", "/temp", "processed", unlock=True)

# information from python.fabio image format
# http://pythonhosted.org/fabio/api/modules.html#id1
"""
var = ['__class__', '__delattr__', '__dict__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__',
       '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__',
       '__subclasshook__', '__weakref__', '_classname', '_compressed_stream', '_need_a_real_file',
       '_need_a_seek_to_read', '_open', '_readheader', 'add', 'area_sum', 'bpp', 'bytecode', 'checkData', 'checkHeader',
       'classname', 'convert', 'currentframe', 'data', 'dim1', 'dim2', 'filename', 'filenumber', 'getclassname',
       'getframe', 'getheader', 'getmax', 'getmean', 'getmin', 'getstddev', 'header', 'header_keys', 'integrate_area',
       'lib', 'load', 'make_slice', 'maxval', 'mean', 'minval', 'nbits', 'next', 'nframes', 'pilimage', 'previous',
       'read', 'readROI', 'readheader', 'rebin', 'resetvals', 'roi', 'save', 'slice', 'stddev', 'toPIL16',
       'update_header', 'write']
"""

# TODO: tester objects in the external threads must their own log files
# TODO: think about ansi vs unicode issues
# TODO: think about data transfer from tiff to nexus if needed - it is possible
# TODO: think about creating an esperanto file if needed

# TODO: There are problems with windows and access files - need to find a solution
# WindowsError - file is used by another process