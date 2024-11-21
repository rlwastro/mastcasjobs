This provides a simple interface to the MAST CasJobs server (home of GALEX,
Kepler, the Hubble Source Catalog, PanSTARRS, etc.) using Dan Foreman-Mackey's
`casjobs <https://github.com/dfm/casjobs>`_ interface.

Installation
------------

Install current versions of both the ``mastcasjobs`` and ``casjobs`` modules:

::

    pip install mastcasjobs

If you want to do development on the ``mastcasjobs`` module, clone it and then install it using:

::

    pip install .

Usage
-----

Here is an example query that does a cone search for PS1 objects within
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

    # user is your MAST Casjobs username
    # pwd is your Casjobs password
    # These can also come from the CASJOBS_USERID and CASJOBS_PW environment variables,
    # in which case you do not need the username or password parameters.
    # Create a Casjobs account at <https://mastweb.stsci.edu/ps1casjobs/CreateAccount.aspx>
    #   if you do not already have one.

    user = "myusername"
    pwd = "My super secret password"

    jobs = mastcasjobs.MastCasJobs(username=user, password=pwd, context="PanSTARRS_DR2")
    results = jobs.quick(query, task_name="python cone search")
    print(results)

Note that the results of the ``quick`` query are by default returned as an
`astropy table <https://docs.astropy.org/en/stable/table/index.html>`_.
You can add the optional parameter ``astropy=False`` to get a string instead.

The ``jobs`` object has other useful methods that allow you to do almost all the queries that you
can run using the web interface.  Use ``help(jobs.function)`` to get details.  Some of the commonly used
functions include:

+-------------------------------------+----------------------------------------------------------------------------------------------+
| Functions                           | Description                                                                                  |
+=====================================+==============================================================================================+
| ``quick``                           | Run short queries that execute in less than 1 minute.                                        |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``submit``                          | Submit a long-running query.                                                                 |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``status``, ``monitor``, ``cancel`` | Monitor a submitted query.                                                                   |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``fast_table``                      | Fast retrieval of data from a MyDB table (works only on MAST Casjobs).                       |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``get_table``                       | Retrieve a small or large MyDB table (slower but works in other Casjobs installations too).  |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``list_tables``                     | List tables in MyDB (or in another context).                                                 |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``drop_table_if_exists``            | Delete a table from your MyDB (if it exists).                                                |
+-------------------------------------+----------------------------------------------------------------------------------------------+
| ``upload_table``                    | Upload a table to your MyDB database.                                                        |
+-------------------------------------+----------------------------------------------------------------------------------------------+

Requirements
------------

This relies on the ``casjobs`` (version 0.0.2 or newer) and ``requests`` modules.

Since Python 2.7 is no longer supported, the installation of this version of the software requires 
Python 3.5 or greater.  The software actually still runs in Python 2.7, but you will have to install it 
manually.

Release Notes
-------------

+-----------+--------------+----------------------------------------------------------------------------------------------------+
| Release   | Date         | Description                                                                                        |
+===========+==============+====================================================================================================+
| ``0.0.7`` | Nov 21, 2024 |  Use POST for queries by default, which avoids limits on the length of query strings.              |
+-----------+--------------+----------------------------------------------------------------------------------------------------+
| ``0.0.6`` | Apr 24, 2024 |  Modified `upload_table()` method in MastCasJobs to allow an `astropy.table` as the data argument. |
+-----------+--------------+----------------------------------------------------------------------------------------------------+
| ``0.0.5`` | Jul 28, 2022 |  Initial pip-installable version.                                                                  |
+-----------+--------------+----------------------------------------------------------------------------------------------------+

License
-------

MIT
