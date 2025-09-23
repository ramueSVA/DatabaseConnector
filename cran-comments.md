This is a major release, with 6 changes and 5 fixes (see NEWS.md).

All reverse dependency issues should now be resolved.

---

## Test environments
* Ubuntu 22.04, R 4.5.1
* MacOS, R 4.5.1
* MacOS, 4.4.1
* Windows 10, R 4.5.1

## R CMD check results

There were no ERRORs or WARNINGs. 

## Downstream dependencies

DatabaseConnector is used by Achilles, CohortAlgebra, CohortExplorer, TreatmentPatterns, and CDMConnector, which were tested with this new version. No issues were found.