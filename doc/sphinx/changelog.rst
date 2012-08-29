FileMover release notes
=======================

Release 1.x.x series
--------------------

1.1.X

  - Fix update status with premature ending of web-session, ticket #3389
  - remove dependency on webtools-base
  - use development as version value in __init__.py and replace it with
    actual version at RPM build stage
  - add cleaner script, it reads input directory and printous FM files older
    then certain threshold, ticket #963
  - fix issue with status update for external requests, ticket #2908
  - Move to standard exception handling
  - throw appropriate message when LFN is not found on any site, ticket #2769
  - Remove hard-coded phedex URL from the code; move it into configuration; ticket #2753
  - replace getLFNSize with phedex call instead of DBS-DD, ticket #2754

1.0.X

  - adjust all code to score 8/10 in pylint tests
  - perform code clean-up and re-factoring with respect to WMCore guidelines and
    CodeAudit, ticket #295
  - migrate FM into WMCore.WebTools framework
  - remove pick-event interface
  - remove all HTML snippets from FileMoverService code into separate templates
  - sanitize all FileMover templates, ticket #740
  - validate input parameters, ticket #741
  - Remove uniqueList; remove getArgs; clean-up DBSInteraction; 
    move lfn.strip into checkargs; remove lfn.unquote(); remove sendEmail; 
  - simplify setStat; rename getStat into updateUserPage; adjust code to always set
    status as tuple(statusCode, statusMsg); update StatusCode/StatusMsg classes;
