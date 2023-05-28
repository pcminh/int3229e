#!/bin/bash

export $(grep -v '^#' .env | xargs)

chi_url=https://data.cityofchicago.org/api/views
declare -A chicago_data=( ["crashes"]="85ca-t3if" ["streets"]="i6bp-fvbx" ["redlight_cam"]="spqx-js37" ["speed_cam"]="hhkd-xvj4" ["traffic_hist"]="sxs8-h27x")
# declare -A chicago_data=( ["crashes"]="85ca-t3if" ["streets"]="i6bp-fvbx" ["redlight_cam"]="spqx-js37" ["speed_cam"]="hhkd-xvj4")

function get_to_local () {
    printf $DATASET_LOCATION_TYPE
    for data in "${!chicago_data[@]}";
    do
        echo $data
        mkdir -p $DATASET_ROOT_PATH/$data
        curl ${chi_url}/${chicago_data[$data]}/rows.csv -o $DATASET_ROOT_PATH/$data/$data.csv
    done

    return
}

function get_to_hdfs () {
    echo $DATASET_LOCATION_TYPE
    for data in "${!chicago_data[@]}";
    do
        echo $data
        hdfs dfs -mkdir -p $DATASET_ROOT_PATH/$data
        curl ${chi_url}/${chicago_data[$data]}/rows.csv | hdfs dfs -put -f - $DATASET_ROOT_PATH/$data/$data.csv
    done

    return
}

function main () {

    if [[ ! -v $DATASET_ROOT_PATH ]] 
    then 
        echo "\$DATASET_ROOT_PATH is not set"
        return
    fi

    if [[ ! -v $DATASET_LOCATION_TYPE ]]
    then 
        echo "\$DATASET_LOCATION_TYPE must be set to one of (hdfs, file, local)"
        return
    fi

    if [[ $DATASET_LOCATION_TYPE == "hdfs" ]]
    then 
        get_to_hdfs
    elif [[ $DATASET_LOCATION_TYPE == "file" ]] || [[ $DATASET_LOCATION_TYPE == "local" ]]
    then
        get_to_local 
    fi
}