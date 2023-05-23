  
#!/bin/bash

# 1.getData.sh
# Downloads data from Chicago Data Portal

chi_url=https://data.cityofchicago.org/api/views
# declare -A chicago_data=( ["crashes"]="85ca-t3if" ["streets"]="i6bp-fvbx" ["redlight_cam"]="spqx-js37" ["speed_cam"]="hhkd-xvj4" ["traffic_hist"]="sxs8-h27x")
declare -A chicago_data=( ["crashes"]="85ca-t3if" ["streets"]="i6bp-fvbx" ["redlight_cam"]="spqx-js37" ["speed_cam"]="hhkd-xvj4")
for data in "${!chicago_data[@]}";
do
	printf $data
	curl ${chi_url}/${chicago_data[$data]}/rows.csv -o ./data/raw/traffic/$data/$data.csv
done