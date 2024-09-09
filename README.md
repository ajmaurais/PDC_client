# Usage

```
usage: PDC_client <command> [<args>]

Available commands:
   studyID         Get the study_id from the pdc_study_id.
   PDCStudyID      Get the pdc_study_id from the study_id.
   studyName       Get the study name.
   metadata        Get the metadata for files in a study.
   metadataToSky   Convert a metadata tsv or json to Skyline annotation csv.
   file            Download a single file.
   files           Download all the files in a study.

Command line client for NCI Proteomics Data Commons

positional arguments:
  command             Subcommand to run.

options:
  -h, --help          show this help message and exit
  --debug {pdb,pudb}  Start the main method in selected debugger
```

# Example

```bash
PDC_client studyID PDC000200
PDC_client metadata 3c0a00b6-154c-11ea-9bfa-0a42f3c845fe
```
