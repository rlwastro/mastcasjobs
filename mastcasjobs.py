"""
Interface to MAST CasJobs using Dan Foreman-Mackey's casjobs.py module

vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 :
"""

from casjobs import CasJobs
import astropy, numpy, time, sys, os, re, requests
from astropy.table import Table
from astropy.io import fits, ascii
from collections import defaultdict
import xml.etree.ElementTree as ET

__all__ = ["MastCasJobs", "contexts"]

# some common MAST database contexts
contexts = [
            "GAIA_DR1",
            "GALEX_Catalogs",
            "GALEX_GR6Plus7",
            "GALEX_UV_BKGD",
            "HLSP_47Tuc",
            "HSLP_GSWLC",
            "HSCv3",
            "HSCv2",
            "HSCv1",
            "Kepler",
            "PanSTARRS_DR1",
            "PanSTARRS_DR2",
            "PHATv2",
            "SDSS_DR12",
            ]

class MastCasJobs(CasJobs):
    """
    Wrapper around the MAST CasJobs services.

    ## Keyword Arguments

    * `username` (str): Your CasJobs user name. It can also be
      provided by the `CASJOBS_USERID` environment variable.
    * `password` (str): Your super-secret CasJobs password. It can also be
      provided by the `CASJOBS_PW` environment variable.
    * `userid` (int): The WSID from your CasJobs profile. This is not used
      if `username` is specified.  Note that when this alternate login method is
      used, the get_fast_table method will not work.  This can also be
      provided by the `CASJOBS_WSID` environment variable.
    * `request_type` (str): The type of HTTP request to use to access the
      CasJobs services.  Can be either 'GET' or 'POST'.  Typically you
      may as well stick with 'GET', unless you want to submit some long
      queries (>~2000 characters or something like that).  In that case,
      you'll need 'POST' because it has no length limit.
    * `context` (str): Default context that is used for queries.
    * `base_url` (str): The base URL that you'd like to use depending on the
      service that you're accessing. Default is the MAST CasJobs URL.
    * `wsid_url` (str): The service URL that is used to login using the username
      and password.  Default is the MAST CasJobs service URL.
    * `fast_url` (str): The URL that provides fast non-queued retrieval for MAST
      CasJobs tables.  Note that this is a non-standard method that only works
      for MAST databases!

    """
    def __init__(self, username=None, password=None, userid=None,
                 request_type="GET", context="PanSTARRS_DR1",
                 base_url="http://mastweb.stsci.edu/ps1casjobs/services/jobs.asmx",
                 wsid_url=None, fast_url=None):

        # get userid and password
        # order of preference: (1) username (2) userid (3) CASJOBS_USERID (4) CASJOBS_WSID (5) prompt
        if username is None:
            if userid is None:
                username = os.environ.get('CASJOBS_USERID')
                if username is None:
                    userid = os.environ.get('CASJOBS_WSID')
        if username is None and userid is None:
            raise ValueError("Specify username or set CASJOBS_USERID environment variable")
        if password is None:
            password = os.environ.get('CASJOBS_PW')
            if password is None:
                raise ValueError("Specify password or set CASJOBS_PW environment variable")
        if base_url.lower().find("//mastweb.stsci.edu/") >= 0:
            # set defaults for MAST CasJobs
            if wsid_url is None:
                wsid_url="https://mastweb.stsci.edu/ps1casjobs/casusers.asmx/GetWebServiceId"
            if fast_url is None:
                fast_url="https://ps1images.stsci.edu/cgi-bin/quick_casjobs.cgi"
        self.username = username
        self.password = password
        self.wsid_url = wsid_url
        self.fast_url = fast_url
        if username is not None:
            userid = self.get_wsid()

        super(MastCasJobs,self).__init__(userid=userid, password=password,
                base_url=base_url, request_type=request_type, context=context)

    def list_tables(self, context="MYDB"):
        """
        Lists the tables in mydb (or other context).

        ## Keyword Arguments

        * `context` (str): Casjobs context used for this query.

        ## Returns

        * `tables` (list): A list of strings with all the table names from mydb.

        """
        q = 'SELECT Distinct TABLE_NAME FROM information_schema.TABLES'
        res = self.quick(q, context=context, task_name='listtables', system=True)
        # the first line is a header and the last is always empty
        # also, the table names have " as the first and last characters
        return [l[1:-1]for l in res.split('\n')[1:-1]]

    def drop_table_if_exists(self, table):
        """
        Drop table from MyDB without an error if it does not exist

        ## Arguments

        * `table` (str): The name of the table to drop.

        """   
        results = self.quick("DROP TABLE IF EXISTS {}".format(table),context="MYDB")

    def quick(self, q, context=None, task_name="quickie", system=False, astropy=True):
        """
        Run a quick job. Like CasJobs method but adds astropy option.

        ## Arguments

        * `q` (str): The SQL query.

        ## Keyword Arguments

        * `context` (str): Casjobs context used for this query.
        * `task_name` (str): The task name.
        * `system` (bool) : Whether or not to run this job as a system job (not
          visible in the web UI or history)
        * `astropy` (bool) : If True, returns output as astropy.Table

        ## Returns

        * `results` (str): The result of the job as a long string (or as Table if astropy=True).

        """
        results = super(MastCasJobs,self).quick(q, context=context, task_name=task_name, system=system)
        if astropy:
            return MastCasJobs.convert_quick_table(results)
        else:
            return results


    def get_wsid(self):

        """
        Returns WSID for this CasJobs username and password
        
        ## Returns

        * `wsid` (str): The WSID for the user

        """   

        if self.wsid_url is None:
            raise ValueError("Specify the wsid_url parameter for CasJobs versions not at MAST")
        r = requests.post(self.wsid_url, data=dict(userid=self.username, password=self.password))
        r.raise_for_status()
        root = ET.fromstring(r.text)
        if root is None or (not root.text) or root.text == "-1":
            raise ValueError("Incorrect MAST CasJobs username/password")
        else:
            return root.text.strip()


    def fast_table(self, table, verbose=False):
        """
        Get a (potentially large) table from CasJobs without going through output queue.
        This only works for MAST CasJobs.

        ## Arguments

        * `table` (str): Name of table in MyDB.

        ## Keyword Arguments

        * `verbose` (bool) : Prints additional information on time to retrieve table.

        ## Returns

        * `results` (astropy.Table): The table as an astropy.Table object.

        """
        if self.fast_url is None:
            raise ValueError("fast_table method is only available for MAST CasJobs")
        if self.username is None:
            raise ValueError("Cannot use fast_table method unless you specify username when accessing CasJobs")
        # make sure the table exists
        try:
            results = self.quick("select top 0 * from {}".format(table),context="MYDB")
        except Exception as e:
            raise ValueError("table MyDB.{} not found".format(table)) from None
        # get table from the quick_casjobs.cgi service
        t0 = time.time()
        r = requests.post(self.fast_url, data=dict(userid=self.username, pw=self.password, table=table))
        if r.status_code == 404:
            raise ValueError("table MyDB.{} not found?".format(table))
        # raise exception on other request errors
        r.raise_for_status()
        if verbose:
            print("{:.1f} s: Retrieved {:.2f}MB table MyDB.{}".format(time.time()-t0,len(r.text)/1.e6,table))
        headline = r.text.split('\n',1)[0]
        names, converters = MastCasJobs.get_converters(headline, delimiter='\t')
        # note that this service replaces NULL entries by default
        tab = ascii.read(r.text,
                         guess=False,format='tab',names=names,converters=converters)
        if verbose:
            print("{:.1f} s: Converted to {} row table".format(time.time()-t0,len(tab)))
        return tab

    def get_table(self, table, format="FITS"):
        """Get possibly large table from CasJobs
        
        This runs a quick job to retrieve the table if it is not too big.
        If it is large, it submits an output job, waits for it complete, 
        and then loads the table.  This is slower than the fast_table method but
        works on other installations of CasJobs (e.g., the SDSS version).
        format can be "FITS" or "CSV".  I think FITS is probably more compact when
        coming over the network but probably takes a bit longer to generate.

        ## Arguments

        * `table` (str): Name of table in MyDB.

        ## Keyword Arguments

        * `format` (str): Format for retrieval ("FITS" or "CSV").

        ## Returns

        * `results` (astropy.Table): The table as an astropy.Table object.

        """
        # make sure the table exists
        try:
            results = self.quick("select top 0 * from {}".format(table),context="MYDB")
        except Exception as e:
            raise ValueError("table MyDB.{} not found".format(table)) from None
        # first try to get it as a quick request, which is much faster if it works
        try:
            return self.quick("select * from {}".format(table),context="MYDB",astropy=True)
        except Exception as e:
            pass
        
        # sigh, have to go through output queue
        format = format.upper()
        if format not in ["FITS","CSV"]:
            # just force a good value
            format = "FITS"
        print("Making output request for {}-format data".format(format))
        job_id = self.request_output(table,format)
        status = self.monitor(job_id)
        if status[0] != 5:
            raise Exception("Output request failed.")
        job_info = self.job_info(jobid=job_id)[0]
        url = job_info["OutputLoc"]
        if format == "FITS":
            fh = fits.open(url)
            tab = Table(fh[1].data)
            fh.close()
        else:
            r = requests.get(url)
            r.raise_for_status()
            tab = ascii.read(MastCasJobs.replacenull(r.text),format='csv')
        return tab


    @staticmethod
    def convert_quick_table(result):
        """Convert the CSV format returned from a quick query to an astropy Table
        ## Arguments

        * `result` (str): Output of the quick query.

        ## Returns

        * `results` (astropy.Table): The table as an astropy.Table object.

        """
        headline = result.split('\n',1)[0]
        names, converters = MastCasJobs.get_converters(headline, delimiter=',')
        tab = ascii.read(MastCasJobs.replacenull(result,delimiter=','),
                         guess=False,fast_reader=False,format='csv',
                         names=names,converters=converters)
        return tab

    @staticmethod
    def get_converters(headline, delimiter=','):
        """Return a list of new names and a dict table of converter functions for the columns
        Column descriptions should look like [name]:datatype
        Returns names (list) and converters (dict)

        ## Arguments

        * `headline` (str): First line of sql output with column info in format "[name]:datatype"

        ## Keyword Arguments

        * `delimiter` (str): delimiter between columns

        ## Returns

        * `names` (list), `converters` (dict)

        """
        pat = re.compile(r'\[(?P<name>[^[]+)\]:(?P<datatype>.+)$')
        # probably need a boolean datatype in this list
        tmap = defaultdict(lambda : numpy.str,
                    int=numpy.int32, smallint=numpy.int16, tinyint=numpy.uint8,
                    bigint=numpy.int64, integer=numpy.int64, bit=numpy.uint8,
                    float=numpy.float64, decimal=numpy.float64, real=numpy.float32,
                    datetime=numpy.datetime64)
        cols = headline.split(delimiter)
        converters = {}
        names = []
        for c in cols:
            m = pat.match(c)
            if not m:
                print("Unable to parse column name '{}'".format(c))
            else:
                newname = m.group('name')
                names.append(newname)
                numpy_type = tmap[m.group('datatype').lower()]
                converters[newname] = [ascii.convert_numpy(numpy_type)]
        return names, converters

    @staticmethod
    def replacenull(results, delimiter=','):
        """Replace strings 'NULL' and 'null' with actual empty strings

        ## Keyword Arguments

        * `delimiter` (str): delimiter between columns

        ## Returns

        * `results` (str): Input modified to remove 'NULL' strings

        """
        if results.startswith('null{}'.format(delimiter)):
            # rare special case
            results = results[4:]
        pat = '(?<=[{}\n])null(?=[{}\n])'.format(delimiter,delimiter)
        results = re.sub(pat, '', results, flags=re.IGNORECASE | re.DOTALL)
        return results

