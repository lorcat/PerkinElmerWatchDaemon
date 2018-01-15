Basic concept:

Step 1:
- Detector collects data and stores it into the RAM drive
  Data is stored into /raw folder of RAM disk (READ Attribute)

- Data is processed
  1. read .tif file, locate its .metadata if possible
  1.1 move the tif and its .metadata into the /temp folder on RAM drive (READ Attribute)

  2. process the metadata file
     extract information given by a separate process
  2.1 pack information from metadata into the .tif file
  2.2 create a nexus file containing relative links to the tif and metadata
  2.3 pack additional information into the nexus file if needed (from memcache)
  2.4 move the set of data preserving the relative file structure into the final directory (READ_WRITE attribute)

  3. move .tif and .metadata from processed folder into the final destination

- Optional
  1. integrate ROI of the image
  1.1 output it as a counter

  2. ZMQ thread with data - ROI or IMAGE

# RAM structure
/raw
/temp
/processed

# Remote storage:
/something

Tech needed:
- fabio python library - external - depends on other
- os+sys - standard
- re python library - standard
- glob python library - standard
- functools - standard
- pytango - external
- h5 library for handling Nexus with links
- pluginbase - external

Possible file processing:
- multiprocessing python library to facilitate as many cores as possible - yes.

# Tango Server attributes

# Related to the RAM DISK
RawDir - READ
TempDir - READ
RemoteDir - READ_WRITE

# Related to the multiprocessing
MaxSubProcess - READ_WRITE - maximum limit of concurrent processes, passed to the child workers

# related to the update interval
TickInterval - READ_WRITE - tick interval for the main worker

@ need a command to receive number and list of plugins used, together with their clicks and offsets
@ need a command to receive offset and

# Multiprocessing
- Main single thread ticking at a given interval
  Main thread starts several threads intended for data processing

- Thread locating raw files - locates the file, checks integrity - modification date, size, copies data to /temp
  Each file gets its own temp directory (locking the file directory (1 handle instead of several handles))
  Thread filters useless data (darks and etc) and removes them in the beginning

- Thread merging data, copies data to /processed
  This thread spawns the multiprocess of data conversion
  Thread spawns up to Maximum limit of concurrent processes - attribute
  Data is passed through a Queue

- Thread locating processed files
  This thread spawns the process/thread of file moving with Maximum limit of concurrent processes
  The tree is not merged if there is a .lock extension

Worker threads start to work unless there is a lock
All worker threads receive a copy for the attributes for the local and the remote directories
Each worker thread can be a plugin - I can still use this template.
Each worker receives information on the max_proc it can spawn for a given task (file cleaning and moving are considered as different tasks).
Still the tasks are most probably can be run sequentially.

# handling files
Rules:
The file must be greater than certain size. Tiffs are usually large, meaning that we require a threshold (ReadOnly Attribute?)
The date of modification must be later than a certain threshold (ReadOnly Attribute?) - 1s?
One could use try: except with fabio library - but it may not be reliable and increase an overhead time for processing
Every file has its own temporary folder for simplicity?

###
# Progress
###
2017-12-21/22
Done all three parts
