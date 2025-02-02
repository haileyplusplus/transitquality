#!/bin/sh
test -e "Chicago.osm.pbf" || curl -O https://download.bbbike.org/osm/bbbike/Chicago/Chicago.osm.pbf
test -e "google_transit.zip" || curl -O https://www.transitchicago.com/downloads/sch_data/google_transit.zip
