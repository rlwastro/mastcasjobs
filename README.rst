This provides a simple interface to the MAST CasJobs server (home of GALEX,
Kepler, the Hubble Source Catalog, PanSTARRS, etc.) using Dan Foreman-Mackey's
`casjobs <https://github.com/dfm/casjobs>`_ interface.

Usage
-----

Install current versions of both modules:

::

    pip install git+git://github.com/dfm/casjobs@master
    pip install git+git://github.com/rlwastro/mastcasjobs@master

Note that this uses some features that are not in the standard pip
version of the casjobs module, so it will probably not work using
a simple 'pip install casjobs'.

An example query that does a cone search for PS1 objects within
50 arc-sec of coordinates RA=187.706, Dec=12.391 (in degrees):

::

    import mastcasjobs

    query = """select o.objID, o.raMean, o.decMean,
    o.nDetections, o.ng, o.nr, o.ni, o.nz, o.ny,
    m.gMeanPSFMag, m.rMeanPSFMag, m.iMeanPSFMag, m.zMeanPSFMag, m.yMeanPSFMag
    from fGetNearbyObjEq(187.706,12.391,50.0/60.0) nb
    inner join ObjectThin o on o.objid=nb.objid and o.nDetections>1
    inner join MeanObject m on o.objid=m.objid and o.uniquePspsOBid=m.uniquePspsOBid
    """

    # get your WSID from from <http://mastweb.stsci.edu/ps1casjobs/changedetails.aspx> after you login to Casjobs
    # pwd is your Casjobs password
    # These can also come from the CASJOBS_WSID and CASJOBS_PW environment variables
    wsid = 265306138
    pwd = "My super secret password"

    jobs = mastcasjobs.MastCasJobs(userid=wsid, password=pwd)
    results = jobs.quick(query, task_name="python cone search")
    print results

License
-------

MIT
