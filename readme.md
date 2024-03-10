# Readme

## Dayly report on Top 10 best-selling resources in each category

Follow these steps to execute **top10.py** in DataProc:

1. select your project:
gcloud config set project stripe-big-3

2. enable API for Cloud Storage and Dataproc

3. create a cluster:
`gcloud dataproc clusters create dataproc1 --region us-east1 --single-node\
    --image-version=2.0 \
    --properties=#dataproc:pip.packages='flask==2.0.2,Werkzeug==2.2.2,google-cloud-storage==1.43.0,google-cloud-firestore==2.3.4,google-cloud-error-reporting==1.4.1'`

4. submit job:
`gcloud dataproc jobs submit pyspark top10.py --cluster dataproc1 --region us-east1`

5. delete the cluster:
`gcloud dataproc clusters delete dataproc1 --region=us-east1`

## Royalties



## Monthly reports on platform usage by resource for country and for time zone

Follow these steps to execute **platform_usage.py** in DataProc:

1. select your project:
gcloud config set project stripe-big-3

2. enable API for Cloud Storage and Dataproc

3. create a cluster:
`gcloud dataproc clusters create dataproc3 --region us-east1 --single-node\
    --image-version=2.0 \
    --properties=#dataproc:pip.packages='flask==2.0.2,Werkzeug==2.2.2,google-cloud-storage==1.43.0,google-cloud-error-reporting==1.4.1'`

4. submit job:
`gcloud dataproc jobs submit pyspark platform_usage.py --cluster dataproc3 --region us-east1`

5. delete the cluster:
`gcloud dataproc clusters delete dataproc3 --region=us-east1`
