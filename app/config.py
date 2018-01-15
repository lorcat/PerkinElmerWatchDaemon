import os

# main controls over the server tick tack - number used for division of a second 10=0.1s step - 10Hz
DAEMON_MULTIPLIER = 3
DAEMON_TICKTACK = 1.

# main directory of the application folder for common use
DIR_APP = os.path.dirname(__file__)

CONFIG_INI = os.path.join(DIR_APP, "config.ini")
CONFIG_INI_RAW = os.path.join(DIR_APP,  "data", "raw")
CONFIG_INI_TEMP = os.path.join(DIR_APP, "data", "temp")
CONFIG_INI_PROC = os.path.join(DIR_APP, "data", "proc")
CONFIG_INI_OUTPUT_ROOT = os.path.join(DIR_APP, "data", "output")
CONFIG_INI_MAXPROC = 5

CFG_SECTION = "Configuration"
CFG_RAWDIR = "raw_dir"
CFG_TEMPDIR = "temp_dir"
CFG_PROCDIR = "proc_dir"
CFG_OUTDIR = "output_dir"
CFG_OUTROOT = "output_root"
CFG_MAXPROC = "maxproc"

# DIR_TEMPFILES - directory which can be considered external to the app
# if it does not exist - the DIR_LOCKFILES will be used instead
DIR_TEMPFILES = os.path.join(DIR_APP, "tmp")
DIR_LOCKFILES = os.path.join(DIR_APP, "locks")