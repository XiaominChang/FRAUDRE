#!/bin/bash

function usage()
{
    echo "Batch Score a particular Model from the AI Solution Catalogue in Google Cloud Platform."
    echo ""
    echo "./build.sh"
    echo "-h --help"
    echo ""
    echo "--amm-id  = '301'    unique identifier for model on the AI Solution Catalogue"
    echo "See: https://confluence.iag.com.au/display/CLA/AI+Solution+Catalogue"
    echo ""
    echo "--dept-and-em = 'dia_em_name'"
    echo "Dept to which the model belongs to and the EM name. All characters must be lower case and underscore."
    echo ""
    echo "--language = 'r'"
    echo "The language of your model, can be R or Python."
    echo ""
    echo "--s-number = 'r'"
    echo "Your IAG s number"
    echo ""
}

NUM_ARGS="$#"
P="python"
R="r"
COMMIT_HASH=$(git rev-parse HEAD)
shopt -s nocasematch

# Loops through arguments of form --KEY = VALUE
# Equal-sign must have at least 1 space surrounding it
while [ $# -gt 0 ]; do
    case $1 in
        -h | --help)
            usage
            exit
            ;;
        -a | --amm-id)
            shift
            MODEL_ID=$1
            shift
            ;;
        -d | --dept-and-em)
            shift
            GM=$1
            shift
            ;;
        -l | --language)
            shift
            LANGUAGE=$1
            shift
            ;;
        -s | --s-number)
            shift
            USER_ID=$1
            shift
            ;;
        -n | --name)
            shift
            NAME=$1
            shift
            ;;
        *)
    
            echo "ERROR: unknown parameter \"$1\""
            usage
            exit 1
            ;;
    esac
done


if [ -z ${MODEL_ID+x} ]; then echo "ERROR - model ID is not set, please set the model id with the --amm-id flag" && exit 1; fi
if [ -z ${GM+x} ]; then echo "ERROR - Department and EM is not set, please set the project id with the --dept-and-em flag" && exit 1; fi
if [ -z ${LANGUAGE+x} ]; then echo "ERROR - Language is not set, please set the model language with the --language flag. eg. python, r" && exit 1; fi
if [ -z ${USER_ID+x} ]; then echo "ERROR - S number is not set, please set the user id with the --s-number flag." && exit 1; fi
if [ -z ${NAME+x} ]; then echo "ERROR - Name is not set, please set your name with the --name flag. eg. Moe" && exit 1; fi


if [[ $MODEL_ID =~ [^[:digit:]] ]]; then
    echo "Invalid Model ID provided. Only the number is to be provided. eg. amm-144, only 144 is to be provided"
    exit 1
fi


if [[ ${LANGUAGE} == ${P} ]]; then
    # Export environment information
    pip3 freeze > requirements.txt
    DOCKERFILE="Dockerfile.Python"
elif [[ ${LANGUAGE} == ${R} ]]; then
    R -e "renv::snapshot()"
    DOCKERFILE="Dockerfile.R"
else
    echo "Invalid language provided"
    exit 1
fi

mkdir -p deployment

for file in `find template -type f`; do (
    if [[ $file == *"id"* ]]; then
        destination=${file//id/$MODEL_ID}
        destination="deployment/$(basename $destination)"
        cp $file $destination
        sed -i '' "s/MODEL_ID/$MODEL_ID/g" $destination
        sed -i '' "s/DEPT_EM/$GM/g" $destination
        sed -i '' "s/COMMIT_HASH/$COMMIT_HASH/g" $destination
        sed -i '' "s/USER_NAME/$NAME/g" $destination
    fi
)
done


docker build -f $DOCKERFILE \
    --build-arg HTTP_PROXY \
    --build-arg http_proxy \
    --build-arg HTTPS_PROXY \
    --build-arg https_proxy \
    --build-arg no_proxy \
    -t private.docker.nexus3.auiag.corp/analytics-platform/images/models/amm-$MODEL_ID:$COMMIT_HASH .

docker login -u $USER_ID nexus3.auiag.corp
docker push private.docker.nexus3.auiag.corp/analytics-platform/images/models/amm-$MODEL_ID:$COMMIT_HASH