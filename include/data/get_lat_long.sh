#!/bin/bash
curl -L -o ./latitude_longitude_data.zip \
  https://www.kaggle.com/api/v1/datasets/download/paultimothymooney/latitude-and-longitude-for-every-country-and-state

unzip ./latitude_longitude_data.zip
rm ./latitude_longitude_data.zip
mv world_country_and_usa_states_latitude_and_longitude_values.csv latitude_longiture_data.csv