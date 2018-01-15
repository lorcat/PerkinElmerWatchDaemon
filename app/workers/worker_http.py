__author__ = "Konstantin Glazyrin (lorcat@gmail.com)"

import os
import sys
import re
import socket
import logging
import httplib, urllib, time
import xml.etree.ElementTree as ET

from app.common import *

# global parameters accessible through the script parameters
HOST, PORT, URL = None, None, None

class HTTPWorker(MutexLock):
    # main parameters - which can be substituted by the global ones
    HOST = "hasyvac.desy.de"
    PORT = 8080

    # output filename
    FILENAME_XML = "{}{}".format(__file__.replace(".py", ""), ".xml")

    # secondary parameters for internal use
    _GET_REQUEST = "GET"
    _POST_REQUEST = "POST"

    def __init__(self, init_page=None, def_file=None, debug_level=None, host=None, port=None):
        super(HTTPWorker, self).__init__(def_file=def_file, debug_level=debug_level)

        # initialization page  - different for all
        self.init_page = init_page

        # check configuration parameters
        self.prepare_host(host)
        self.prepare_port(port)
        self.prepare_webpage()

    def prepare_host(self, host=None):
        global HOST
        try:
            if self.test(HOST):
                self.HOST = HOST
            if self.test(host):
                self.HOST = host
        except NameError:
            self.debug("global HOST is not defined")
        finally:
            self.debug("Using the host ({})".format(self.HOST))

    def prepare_webpage(self):
        global URL
        try:
            if self.test(URL):
                self.init_page = URL
        except NameError:
            self.debug("global WEBPAGE is not defined")
        finally:
            self.debug("Using the WEBPAGE ({})".format(self.init_page))

    def prepare_port(self, port=None):
        global PORT
        try:
            if self.test(PORT):
                self.PORT = PORT
            if self.test(port):
                self.PORT = port
        except NameError:
            self.debug("global PORT is not defined")
        finally:
            self.debug("Using the port ({})\n".format(self.PORT))

    def start(self):
        """
        Starts the main procedure by opening the socket
        :return: 
        """
        res = None
        # test for the successful connection - server must be online
        conn = self.start_connection()

        try:
            if not self.test(conn):
                raise IOError

            # obtaining sid
            sid = self.request_sid()

            if not self.test(sid):
                raise IOError

            # perform some sleep before the next operation
            time.sleep(0.2)

            # obtain the data response
            data = self.request_params(sid)

            # parse the xml, save the xml
            self.request_xml_file(data)
            res = self.request_xml(data)

            # obtaining the data
        except IOError:
            self.error("Some error")

        # returns the ET.root or None
        return res


    def start_connection(self):
        """
        Prepares the socket and tests it
        :return: None or Connection
        """
        res = None

        conn = httplib.HTTPConnection(self.HOST, self.PORT)
        try:
            self.debug("Opening the HTTP connection to ({}:{})".format(self.HOST, self.PORT))
            conn.request(self._GET_REQUEST, "/")
            response = conn.getresponse()
            self.debug("Initial socket response: ({}:{}); Normal value (404:Not Found)".format(response.status, response.reason))
            self.debug("Connected to ({}:{})".format(self.HOST, self.PORT))
            self.debug("Closing the socket")
            conn.close()
            res = True
        except socket.error:
            self.error("Could not open socket ({}:{})".format(self.HOST, self.PORT))
        finally:
            return res

    def cleanup(self, conn):
        """
        Cleaning up the connection
        :param conn: 
        :return: 
        """
        if self.test(conn):
            self.debug("Closing the HTTP connection")
            conn.close()

    def request_sid(self):
        """
        Initial request establishing the sessionId
        :return: 
        """
        res = None
        self.debug("Establishing initial connection and sessionId with URL ({})".format(self.init_page))
        conn = httplib.HTTPConnection(self.HOST, self.PORT)
        conn.request(self._GET_REQUEST, self.init_page)
        response = conn.getresponse()
        try:
            if response.status != 200:
                raise ValueError

            # obtaining the sid
            patt = re.compile("sessionId\s+=\s+([0-9]+)")
            data = response.read()

            if not self.test(data) or len(data) == 0:
                raise ValueError

            match = patt.search(data)
            if not self.test(match):
                raise ValueError

            res = match.group(1)
            self.debug("Obtained an sessionId ({})".format(res))

        except ValueError:
            self.error("Unusual response received ({}:{})".format(response.status, response.reason))
        finally:
            self.cleanup(conn)
            return res

    def request_params(self, sid):
        """
        Prepare a new request with an sid - obtain parameters from the page
        :param sid: 
        :return: 
        """

        res = None
        self.debug("Establishing the connection with sessionId ({}) and obtain the beamline parameters".format(sid))

        # establish a connection and get a response
        conn = httplib.HTTPConnection(self.HOST, self.PORT)
        params = urllib.urlencode({'param': '<update><sessionId>{}</sessionId><timeStamp>{}</timeStamp></update>'.format(
                sid, self.get_timestamp())})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "*/*"}

        timestamp = int(time.time() * 1000)
        url = '/web2cToolkit/Web2c?param{}'.format(urllib.urlencode(
            {'': '<update><sessionId>{}</sessionId><timeStamp>{}</timeStamp></update>'.format(sid, timestamp)}))
        url = url.replace('%2F', '/')
        conn.request("POST", url, params, headers=headers)

        response = conn.getresponse()
        try:
            if response.status != 200:
                raise ValueError

            # obtaining the data
            data = response.read()
            res = urllib.unquote(data).replace("</style></style>", "</style>")
            self.debug("New data is received ({})".format(res))

        except ValueError:
            self.error("Unusual response received ({}:{})".format(response.status, response.reason))
        finally:
            self.cleanup(conn)
            return res

    def request_xml(self, data):
        """
        Parses the XML tree for the obtained data
        :return: 
        """
        res = None
        try:
            # get response
            root = ET.fromstring(data)
            res = root
        except ET.ParseError as e:
            self.error("Could not process data - invalid response from the server: {}".format(e))
        return res

    def request_xml_file(self, data):
        """
        Dumps the data into a human readable format
        :return: 
        """
        try:
            if os.path.exists(self.FILENAME_XML) and not os.path.isfile(self.FILENAME_XML):
                raise ValueError

            fh = open(self.FILENAME_XML, "w")
            data = data.replace("</update>", "</update>\n")
            fh.write(data)
            fh.close()
        except ValueError:
            self.error("Could not save the data intor the file ({})".format(self.FILENAME_XML))

    def get_timestamp(self):
        """
        Returns the timestamp
        :return: 
        """
        return int(time.time()*1000)

def usage():
    print("""{} [--help] [--host=HOST] [--port=PORT] [--url=URL]
    HOST - host used for HTTP connection
    PORT - port used for HTTP connection
    URL - initial url used for the beamline parameter extraction
    """.format(__file__))

def prep_sysargs():
    """
    Converts the program arguments into the script parameters
    :return: 
    """

    global HOST, PORT, URL
    if len(sys.argv) > 0:
        try:
            if "--help" in sys.argv or "-h" in sys.argv:
                raise ValueError

            t = Tester()
            for arg in sys.argv:
                phost = re.compile("--host=(.*)")
                pport = re.compile("--port=(.*)")
                purl = re.compile("--url=(.*)")

                mhost = phost.search(arg)
                if mhost:
                    HOST = mhost.group(1)
                    t.debug("Host is changed to ({})".format(HOST))

                mport = pport.search(arg)
                if mport:
                    PORT = mport.group(1)
                    t.debug("PORT is changed to ({})".format(PORT))

                murl = purl.search(arg)
                if murl:
                    URL = murl.group(1)
                    t.debug("URL is changed to ({})".format(URL))

        except ValueError:
            usage()

def main():
    prep_sysargs()
    worker = HTTPWorker("/web2cToolkit/Web2c?param=(open)p3_02info.xml,0")
    root = worker.start()

    if worker.test(root):
        worker.info("The worker has successfully finished the tasks")
    else:
        worker.info("Unfortunately, the worker has not successfully finished the tasks")

if __name__ == "__main__":
    main()