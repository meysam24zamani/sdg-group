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
echo "===> Running docker image ($IMAGE_TAG)..."

docker run \
    -it \
    -v $(pwd)/scripts/$SCRIPT1:$WORK_DIRECTORY/$SCRIPT1 \
    -v $(pwd)/scripts/$SCRIPT2:$WORK_DIRECTORY/$SCRIPT2 \
    $IMAGE_TAG \
    spark-submit \
    --master spark://spark:7077 \
    $SCRIPT1

## Direct_Run_Terminal => docker run -it -v $(pwd)/process-single-forecast.py:/usr/src/app/process-single-forecast.py -v $(pwd)/process-individual-stores-items.py:/usr/src/app/process-individual-stores-items.py meysam24zamani/spark-driver:v1.0.0 spark-submit --master spark://spark:7077 process-individual-stores-items.py
