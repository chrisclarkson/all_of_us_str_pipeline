import pandas
import os

# This query represents dataset "late_onset_ataxia_patients_wgs" for domain "survey" and was generated for All of Us Controlled Tier Dataset v7
dataset_59262488_survey_sql = """
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.survey,
        answer.question_concept_id,
        answer.question,
        answer.answer_concept_id,
        answer.answer,
        answer.survey_version_concept_id,
        answer.survey_version_name  
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.ds_survey` answer   
    WHERE
        (
            question_concept_id IN (SELECT
                DISTINCT concept_id                         
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c                         
            JOIN
                (SELECT
                    CAST(cr.id as string) AS id                               
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr                               
                WHERE
                    concept_id IN (1586134, 1740639, 43528895, 1585710)                               
                    AND domain_id = 'SURVEY') a 
                    ON (c.path like CONCAT('%', a.id, '.%'))                         
            WHERE
                domain_id = 'SURVEY'                         
                AND type = 'PPI'                         
                AND subtype = 'QUESTION')
        )  
        AND (
            answer.PERSON_ID IN (SELECT
                distinct person_id  
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (SELECT
                    criteria.person_id 
                FROM
                    (SELECT
                        DISTINCT person_id, entry_date, concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                    WHERE
                        (concept_id IN (4041682) 
                        AND is_standard = 1 )) criteria ) )
        )"""

dataset_59262488_survey_df = pandas.read_gbq(
    dataset_59262488_survey_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_59262488_survey_df.head(5)


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

def modify_jaf(PARAMETER_FILENAME,folders,suffix='.vcf'):
    import os
    import glob
    jaf=pandas.read_csv(PARAMETER_FILENAME,sep='\t')
    already_done=[]
    for folder in folders:
        files=os.popen('gsutil -u $GOOGLE_PROJECT ls '+os.path.join(folder,'*'+suffix)).readlines()
        already_done=already_done+files
    ids=[]
    for file in already_done:
        ids.append(os.path.basename(file.rstrip().replace(suffix,'')))
    jaf['--env PREFIX']=jaf['--env PREFIX'].astype(str)
    jaf=jaf[~jaf['--env PREFIX'].isin(ids)]
    return jaf

jaf=modify_jaf(PARAMETER_FILENAME,['$WORKSPACE_BUCKET/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250314/090850/'],suffix='.vcf')
print(df.shape)
print(jaf.shape)
jaf.to_csv(PARAMETER_FILENAME,sep='\t',index=False)


"""
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
"""
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

%%bash

dstat \
    --provider google-cls-v2 \
    --project "${GOOGLE_PROJECT}" \
    --location us-central1 \
    --jobs "${JOB_ID}" \
    --users "${USER_NAME}" \
    --status '*'




















"""2025-03-10 16:52:19 INFO: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: No URLs matched: /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: 1 file/object could not be transferred.
2025-03-10 16:52:20 WARNING: Sleeping 10s before the next attempt of failed gsutil command
2025-03-10 16:52:20 WARNING: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
2025-03-10 16:52:25 INFO: gsutil -h Content-Type:text/plain -u terra-vpc-sc-d146c399 -mq cp /tmp/continuous_logging_action/stderr gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/notebooks/expansionh--chrisclarkson--250310-163949-65.2-stderr.log
2025-03-10 16:52:25 INFO: gsutil -h Content-Type:text/plain -u terra-vpc-sc-d146c399 -mq cp /tmp/continuous_logging_action/output gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/notebooks/expansionh--chrisclarkson--250310-163949-65.2.log
2025-03-10 16:52:25 INFO: gsutil -h Content-Type:text/plain -u terra-vpc-sc-d146c399 -mq cp /tmp/continuous_logging_action/stdout gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/notebooks/expansionh--chrisclarkson--250310-163949-65.2-stdout.log
2025-03-10 16:52:30 INFO: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: No URLs matched: /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: 1 file/object could not be transferred.
2025-03-10 16:52:31 WARNING: Sleeping 10s before the next attempt of failed gsutil command
2025-03-10 16:52:31 WARNING: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
2025-03-10 16:52:41 INFO: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: No URLs matched: /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: 1 file/object could not be transferred.
2025-03-10 16:52:42 WARNING: Sleeping 10s before the next attempt of failed gsutil command
2025-03-10 16:52:42 WARNING: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
2025-03-10 16:52:52 INFO: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: No URLs matched: /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212
CommandException: 1 file/object could not be transferred.
2025-03-10 16:52:54 ERROR: gsutil  -u terra-vpc-sc-d146c399 -mq cp /mnt/data/output/gs/fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212 gs://fc-secure-24f31006-e5d2-4319-a77f-e991b03a742d/dsub/results/ExpansionHunter-aou_dsub/chrisclarkson/20250310/154709/5521212

"""














