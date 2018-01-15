# PerkinElmerWatchDaemon project (RAM batch process Tango Server) - published under license LGPL

## Purpose
PerkinElmer XRD1621 or a similar device is controled via [QXRD program](http://qxrd.sourceforge.net/)
QXRD produces .TIF and .META files. The amount of information stored by QXRD is limited and does not follow the NeXus or h5 format.

The idea behind the application is follows:
1. QXRD saves files into a RAM drive created by [Imdisk](https://sourceforge.net/projects/imdisk-toolkit/)
2. Daemon is started as a [Tango Server](http://www.tango-controls.org/) and runs in the background
    1. It monitors raw files and cleans unnecessary files (dark files)
    2. It merges the information located in the .META files produced by QXRD into the TIFF file
    3. It creates a NeXuS file containing information on the experiment (by default - basic info from the META)
    4. It outputs the newly processed files into an output folder

## Operation
Daemon runs on the base of intervals - ticks. The configuration of the tick interval is found in app/config.py.
The two parameters - DAEMON_MULTIPLIER and DAEMON_TICKTACK control time to wait between different ticks.

**Time to wait = DAEMON_TICKTACK / DAEMON_MULTIPLIER**

Expiration of the the wait interval triggers a test for additional functionality implemented through plugins.
Each plugin has two settings controlling how often the plugin should fire (in term of time intervals - ticks) and an offset for a first launch.
Each plugin has a locking (file lock) based mechanism, preventing from a launch before the previous instance of the same plugin has finished processing.

Each plugin should be written in such a way that it demonstrates clearly it TICKTACK, TICKTACK_OFFSET parameters and functions entitled 'setup' and 'work'.
TICKTACK controls periodicity of the starting event - e.g. every TICKTACK*DAEMON_TICKTACK, and TICKTACK_OFFSET controls an offset.
More can be found \app\plugins\backup\plugin_test.py

## Specific Python dependencies (modules)
plugin_base, h5, PyTango, fabio

plugin functionality can be expanded, i.e. memcached - for timeout free communication of the external parameters to save and etc.

# Other
While the startup of the daemon is suggested within the Tango Server framework, the server can be also tested and run as an interface free process (control only via /app/config.ini and /app/config.py)