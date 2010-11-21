Introduction
============

FileMover service was designed to allow users to request/transfer data via web interface to local disk. 
Please note that service is designed to prevent large data transfer by applying certain limitations for users on number of LFNs/user and number of requests/day.

Dependencies
------------
FileMover is written in python and relies on standard python modules.
It uses GRID middleware stack, e.g. BNL SRM, LCG, etc., for file transfer across the sites.

Below we list all dependencies clarifying their role for FileMover

- *python*, FileMover is written in python (2.6), see [Python]_;
- *cherrypy*, a generic python web framework, see [CPF]_;
- *yui* the Yahoo YUI Library for building richly interactive web applications,
  see [YUI]_;
- *Cheetah*, a python template framework, used for all DAS web templates, see
  [Cheetah]_;
- *sphinx*, a python documentation library servers all DAS documentation, 
  see [Sphinx]_;
