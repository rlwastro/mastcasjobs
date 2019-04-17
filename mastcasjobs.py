"""
Interface to MAST Casjobs using Dan Foreman-Mackey's casjobs.py module

vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 :
"""

from casjobs import CasJobs
import os, requests
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
    * `base_url` (str): The base URL that you'd like to use depending on the
      service that you're accessing. Default is the MAST Casjobs URL.
    * `wsid_url` (str): The service URL that is used to login using the username
      and password.  Default is the MAST Casjobs service URL.
    * `request_type` (str): The type of HTTP request to use to access the
      CasJobs services.  Can be either 'GET' or 'POST'.  Typically you
      may as well stick with 'GET', unless you want to submit some long
      queries (>~2000 characters or something like that).  In that case,
      you'll need 'POST' because it has no length limit.
    * `context` (str): Default context that is used for queries.

    """
    def __init__(self, username=None, password=None, userid=None,
                 base_url="http://mastweb.stsci.edu/ps1casjobs/services/jobs.asmx",
                 wsid_url="https://mastweb.stsci.edu/ps1casjobs/casusers.asmx/GetWebServiceId",
                 request_type="GET", context="PanSTARRS_DR1"):

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
        self.username = username
        self.wsid_url = wsid_url
        self.password = password
        if username is not None:
            userid = self.get_wsid()

        super(MastCasJobs,self).__init__(userid=userid, password=password,
                base_url=base_url, request_type=request_type, context=context)

    def list_tables(self, context="MYDB"):
        """
        Lists the tables in mydb (or other context).

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

    def get_wsid(self):

        """Returns WSID for this Casjobs username and password
        
        ## Returns

        * `wsid` (str): The WSID for the user
        """   

        r = requests.post(self.wsid_url, data=dict(userid=self.username, password=self.password))
        r.raise_for_status()
        root = ET.fromstring(r.text)
        if root is None or (not root.text) or root.text == "-1":
            raise ValueError("Incorrect MAST Casjobs username/password")
        else:
            return root.text.strip()

