# all_of_us_str_pipeline
This is pipeline for studying STR data on the All of us (AOU) research platform.
It will use ExpansionHunter version 5 to assess repeats at speficified loci on CRAM files.
You will need to setup a google account to access AOU. If you already have one- it may be necessary to set one up exclusively for the AOU access.
When applying- apply for tier 7 or higher access.
## Step 1 cohort selection
![plot](./cohort_page.png)
When you have setup your account and signed in- you'll see a page like this.
You will need to set up a cohort by clicking "+ cohort".
![plot](./cohort_creation_page.png)
Once you've created a cohort with all desired characteristics- you'll then need to create a dataset from said cohort:
![plot](./dataset_creation.png)
This can be done while selecting with the cohorts you've created and annotated with the data you most desire.
When a dataset is created it will then be analysable in a custom named python notebook (.ipynb file)
## Step 2 starting interactive session
To access and analyse the data via the python notebook environment, you'll first need to click the jupyter environment icon.
![plot](./jupyter_notebook_environment.png)
When the jupyter environment is running then switch to the analysis tab and click on the .ipynb file that you want to use to analyse a dataset.
## Step 3(a) CRAM file path assembly
The code needed to assemble the data/individuals requested in the cohort/dataset creation, will have been written at the top of the notebook.
Execute this code and you will see it has been downloaded as per a loading bar.
See the lines 1-62 in `aou_sub_notebook.py`
From here on we can begin to submit jobs to analyse the CRAM files for the individuals in this dataset (stored in the `dataset_59262488_survey_df` variable):

```
from datetime import datetime
USER_NAME = os.getenv('OWNER_EMAIL').split('@')[0].replace('.','-')
%env USER_NAME={USER_NAME}
ids=dataset_59262488_survey_df['person_id'].drop_duplicates()
LINE_COUNT_JOB_NAME='ExpansionHunter-aou_dsub'
%env JOB_NAME={LINE_COUNT_JOB_NAME}
line_count_results_folder = os.path.join(
    os.getenv('WORKSPACE_BUCKET'),
    'dsub',
    'results',
    LINE_COUNT_JOB_NAME,
    USER_NAME,
    datetime.now().strftime('%Y%m%d/%H%M%S'))

crams=[]
crais=[]
prefixes=[]
for id in ids:
    crams.append("gs://fc-aou-datasets-controlled/pooled/wgs/cram/v7_delta/wgs_"+str(id)+".cram")
    crais.append("gs://fc-aou-datasets-controlled/pooled/wgs/cram/v7_delta/wgs_"+str(id)+".cram.crai")
    prefixes.append(str(id).rstrip())

df = pandas.DataFrame(data={
    '--input CRAM': crams,
    '--input CRAI': crais,
    '--env PREFIX': prefixes,
    '--output-recursive OUTPUT_PATH': line_count_results_folder
})

print(df.head())
PARAMETER_FILENAME = 'count_lines_in_files_aou_dsub.tsv'


%env PARAMETER_FILENAME={PARAMETER_FILENAME}
df.to_csv(PARAMETER_FILENAME, sep='\t', index=False)
```
## Step 3(b) running Expansion Hunter
Enter a new cell.
To run jobs a specific aou_sub.sh script is needed:
```
%%writefile ~/aou_dsub.bash

#!/bin/bash

# This shell function passes reasonable defaults for several dsub parameters, while
# allowing the caller to override any of them. It creates a nice folder structure within
# the workspace bucket for dsub log files.

# --[ Parameters ]--
# any valid dsub parameter flag

#--[ Returns ]--
# the job id of the job created by dsub

#--[ Details ]--
# The first five parameters below should always be those values when running on AoU RWB.

# Feel free to change the values for --user, --regions, --logging, and --image if you like.

# Note that we insert some job data into the logging path.
# https://github.com/DataBiosphere/dsub/blob/main/docs/logging.md#inserting-job-data

function aou_dsub () {

  # Get a shorter username to leave more characters for the job name.
  local DSUB_USER_NAME="$(echo "${OWNER_EMAIL}" | cut -d@ -f1)"

  # For AoU RWB projects network name is "network".
  local AOU_NETWORK=network
  local AOU_SUBNETWORK=subnetwork

  dsub \
      --provider google-cls-v2 \
      --user-project "${GOOGLE_PROJECT}"\
      --project "${GOOGLE_PROJECT}"\
      --image 'marketplace.gcr.io/google/ubuntu1804:latest' \
      --network "${AOU_NETWORK}" \
      --subnetwork "${AOU_SUBNETWORK}" \
      --service-account "$(gcloud config get-value account)" \
      --user "${DSUB_USER_NAME}" \
      --regions us-central1 \
      --logging "${WORKSPACE_BUCKET}/dsub/logs/{job-name}/{user-id}/$(date +'%Y%m%d/%H%M%S')/{job-id}-{task-id}-{task-attempt}.log" \
      "$@"
}
```
Then in a different cell submit the jobs to run Expansion hunter:
```
%%bash
source ~/aou_dsub.bash # This file was created via notebook 01_dsub_setup.ipynb.

aou_dsub \
  --logging "gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/notebooks/" \
  --name "${JOB_NAME}" \
  --image "weisburd/expansion-hunters:latest" \
  --input REFERENCE="gs://genomics-public-data/references/hg38/v0/Homo_sapiens_assembly38.fasta" \
  --input REFERENCE_INDEX="gs://genomics-public-data/references/hg38/v0/Homo_sapiens_assembly38.fasta.fai" \
  --input CATALOG="gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/notebooks/variant_catalog_chr.json" \
  --disk-size 256 \
  --tasks "count_lines_in_files_aou_dsub.tsv" \
  --command 'ExpansionHunter \
          --reads ${CRAM} \
          --variant-catalog ${CATALOG} \
          --reference ${REFERENCE} \
          --output-prefix ${OUTPUT_PATH}/${PREFIX}'
```

## Step 4 analysis

