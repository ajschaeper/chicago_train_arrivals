#!/bin/bash

################################################################################
### CREATE CONFIG FILE
################################################################################

# config 
CONF_FILE_PREFIX="./conf"
CONF_FILE_TEMPLATE=${CONF_FILE_PREFIX}".json_template" 
CONF_FILE_TMP=${CONF_FILE_PREFIX}".json_tmp" 
CONF_FILE=${CONF_FILE_PREFIX}".json" 

# create new config from template
echo -n "Creating config file \"${CONF_FILE}\" from \"${CONF_FILE_TEMPLATE}\"... "
cp ${CONF_FILE_TEMPLATE} ${CONF_FILE} 
echo "done"

################################################################################
### PARSE STATION FILE TO CREATE TOPICS
################################################################################

STATIONS_FILE="producers/data/cta_stations.csv"
echo -n "Reading stations for topics from \"${STATIONS_FILE}\"... "

# read station name, cleanse and sort data
STATIONS=$(cat ${STATIONS_FILE} \
	| awk -F ',' '{print $4}' \
	| sed -e "s/\//_and_/g" \
	| sed -e "s/\ /_/g" \
	| sed -e "s/-/_/g" \
	| sed -e "s/'//g" \
	| tr "[:upper:]" "[:lower:]" \
	| sort -u)

echo "done"

################################################################################
### GENERATE TOPIC LIST
################################################################################

echo -n "Generating topic list... "

# base string to con 
TOPIC_LIST="\"list\": "

### stations

# create topic names from stations
for ST in ${STATIONS}; do
	TOPIC_LIST=${TOPIC_LIST}," \"cta_sn_${ST}\""
done

### turnstiles

# create topic names from turnstiles
for TS in ${STATIONS}; do
	TOPIC_LIST=${TOPIC_LIST}," \"cta_te_${ST}\""
done

### weather

# create topic for weather
TOPIC_LIST=${TOPIC_LIST}," \"cta_weather\""


# adjust start and end of topic list
TOPIC_LIST=$(echo ${TOPIC_LIST} | sed -e "s/\,/\[/")
TOPIC_LIST=${TOPIC_LIST}" ]"

echo "done"

################################################################################
### APPLY CHANGES
################################################################################

# display changes
echo -n "Adjusting config file \"${CONF_FILE}\"... "

# apply changes
cat ${CONF_FILE} | sed -e "s/__topic_list__/${TOPIC_LIST}/" > ${CONF_FILE_TMP} 
mv ${CONF_FILE_TMP} ${CONF_FILE} 

echo "done"

### EOF
