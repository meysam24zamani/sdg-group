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
echo "===> Uploading docker image ($IMAGE_TAG) to private registry..."
docker push $IMAGE_TAG
