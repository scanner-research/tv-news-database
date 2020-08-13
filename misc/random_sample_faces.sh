#!/bin/bash

count=1000

for year in {2010..2020}
do
  for channel in CNN FOXNEWS MSNBC;
  do
    echo Sampling: $year $channel
    ./random_sample_faces.py --year=$year --channel=$channel face_samples/$channel.$year.json -n $count &
  done
  wait
done
