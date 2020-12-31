#!/bin/bash
# Exit script if some step fails
set -e

export VERSION=$(cat names/VERSION.txt)
export IMAGE_NAME=$(cat names/NAME_IMAGE.txt)

export IMAGE_TAG_BODY="meysam24zamani/$IMAGE_NAME"
export IMAGE_TAG="$IMAGE_TAG_BODY:v$VERSION"


echo ""
echo "===> Authenticating to private docker registry using DockerHub..."
eval `docker tag $IMAGE_TAG_BODY $IMAGE_TAG`
docker login --username=meysam24zamani --password=********

echo ""
echo "===> Building docker image ($IMAGE_TAG)"
docker build --pull -t $IMAGE_TAG .


echo ""
echo "===> Running docker image ($IMAGE_TAG)..."
docker run \
    --rm \
    -it \
    --link master:master \
    --volumes-from spark-datastore $IMAGE_TAG_BODY spark-submit \
    --master spark://172.17.0.2:7077 /process.py \
    --name $VERSION-$IMAGE_NAME \
    -p 8070:8080 \
    -d \
    -e LOAD_EX=y \
    -v "$PWD/dags":/usr/local/airflow/dags \
    $IMAGE_TAG