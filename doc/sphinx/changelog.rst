FileMover release notes
=======================

Release 1.x.x series
--------------------
This release is based on CMS COMP CVS tag V01_00_33 as starting point for CVS to SVN
migration.

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
