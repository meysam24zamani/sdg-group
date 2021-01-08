#!/bin/bash
# Exit script if some step fails
set -e

export VERSION=$(cat names/VERSION.txt)
export IMAGE_NAME=$(cat names/NAME_IMAGE.txt)

export IMAGE_TAG_BODY="meysam24zamani/$IMAGE_NAME"
export IMAGE_TAG="$IMAGE_TAG_BODY:v$VERSION"

export WORK_DIRECTORY="/usr/src/app"

export SCRIPT1="process-individual-stores-items.py"
export SCRIPT2="process-single-forecast.py"


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
    -it \
    --name $VERSION-$IMAGE_NAME \
    -v $(pwd)/scripts/$SCRIPT1:$WORK_DIRECTORY/$SCRIPT1 \
    -v $(pwd)/scripts/$SCRIPT2:$WORK_DIRECTORY/$SCRIPT2 \
    $IMAGE_TAG \
    spark-submit \
    --master spark://spark:7077 \
    $SCRIPT1