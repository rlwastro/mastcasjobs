"""
Interface to MAST Casjobs using Dan Foreman-Mackey's casjobs.py module

vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 :
"""

from casjobs import CasJobs

__all__ = ["MastCasJobs", "contexts"]

# some common MAST database contexts
contexts = [
            "GAIA_DR1",
            "GALEX_Catalogs",
            "GALEX_GR6Plus7",
            "GALEX_UV_BKGD",
            "HLSP_47Tuc",
            "HSLP_GSWLC",
            "HSCv2",
            "HSCv1",
            "Kepler",
            "PanSTARRS_DR1",
            "PHATv2",
            "SDSS_DR12",
            ]

class MastCasJobs(CasJobs):
    """
    Wrapper around the MAST CasJobs services.

    ## Keyword Arguments

    * `userid` (int): The WSID from your CasJobs profile. If this is not
      provided, it should be in your environment variable `CASJOBS_WSID`.
    * `password` (str): Your super-secret CasJobs password. It can also be
      provided by the `CASJOBS_PW` environment variable.
    * `base_url` (str): The base URL that you'd like to use depending on the
      service that you're accessing. Default is the MAST Casjobs URL.
    * `request_type` (str): The type of HTTP request to use to access the
      CasJobs services.  Can be either 'GET' or 'POST'.  Typically you
      may as well stick with 'GET', unless you want to submit some long
      queries (>~2000 characters or something like that).  In that case,
      you'll need 'POST' because it has no length limit.
    * `context` (str): Default context that is used for queries.

    """
    def __init__(self, userid=None, password=None,
                 base_url="http://mastweb.stsci.edu/ps1casjobs/services/jobs.asmx",
                 request_type="GET", context="PanSTARRS_DR1"):

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
