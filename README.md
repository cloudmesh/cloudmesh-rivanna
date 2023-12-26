Documentation
=============


[![image](https://img.shields.io/travis/TankerHQ/cloudmesh-rivanna.svg?branch=main)](https://travis-ci.org/TankerHQ/cloudmesn-rivanna)

[![image](https://img.shields.io/pypi/pyversions/cloudmesh-rivanna.svg)](https://pypi.org/project/cloudmesh-rivanna)

[![image](https://img.shields.io/pypi/v/cloudmesh-rivanna.svg)](https://pypi.org/project/cloudmesh-rivanna/)

[![image](https://img.shields.io/github/license/TankerHQ/python-cloudmesh-rivanna.svg)](https://github.com/TankerHQ/python-cloudmesh-rivanna/blob/main/LICENSE)

see cloudmesh.cmd5

* https://github.com/cloudmesh/cloudmesh.cmd5



## Jupyter on rivanna

```
# cms vpn connect
local>
  cms rivanna a100
a100>
  # source your ENV3
  pip install pip -U
  pip install jupyterlab 
  pip install jupyter
```


```
rivanna>
  pip install pip -U
  pip install jupyterlab
  pip install notebook
```



## Manual Page

<!-- START-MANUAL -->
```
Command rivanna
===============

::

  Usage:
        rivanna storage info DIRECTORY [--info]
        rivanna login [--sbatch=SBATCH] [--host=HOST] [KEY] [--debug]
        rivanna slurm info
        rivanna slurm [--sbatch=SBATCH] [--host=HOST] [KEY] [--debug]
        rivanna tutorial [KEYWORD]
        rivanna vpn on
        rivanna vpn off
        rivanna vpn info
        rivanna vpn status
        rivanna ticket
        rivanna image build DEFFILE
        rivanna jupyter --port=PORT

  This command simplifys access to rivanna.

  Arguments:
      DIRECTORY  a location of a directory on rivanna

  Options:
      -f      specify the file

  Description:

    rivanna storage info DIRECTORY [--info]
        A command that can be executed remotely and obtains information about the storage associated
        with a directory on rivanna

    rivanna login [--allocation=ALLOCATION] [--gres=GRES] [--cores=CORES] [--host=HOST] [--constraint=CONSTRAINT] [--reservation=RESERVATION] [--time=TIME]
        A command that logs into from your current computer into an interactive node on rivanna
        with a given GPU. Values for GPU are

        a100 or a100-80 -- uses a A100 with 80GB
        a100-40 -- uses a A100 with 40GB
        a100-localscratch  -- uses a A100 with 80GB and access to localscratch

        others to be added from rivannas hardware description

    rivanna tutorial apptainer
        shows the rivanna apptainer documentation on the UVA website

    rivanna tutorial hpc
        shows the general rivanna hpc information on infomall.org

    rivanna tutorial pod
        shows the general rivanna pod information on infomall.org

    rivanna tutorial globus
        shows the general rivanna globus information on infomall.org

    rivanna tutorial rclone
        shows the general rivanna rclone information on infomall.org

    rivanna vpn on
        switches the vpn on

    rivanna vpn off
        switches the vpn off

    rivanna vpn info
        prints the information about the current connection to the
        internet. INformation includes

          "ip": "128.143.1.11",
          "hostname": "vpn-user-1-11.itc.virginia.edu",
          "city": "Charlottesville",
          "region": "Virginia",
          "country": "US",
          "loc": "38.0293,-78.4767",
          "org": "AS225 University of Virginia",
          "postal": "22902",
          "timezone": "America/New_York",
          "readme": "https://ipinfo.io/missingauth"

    rivanna vpn status
        prints True if vpn is enabled, False if not


  Installation:

    pip install cloudmesh-rivana
    cms help

```
<!-- STOP-MANUAL -->