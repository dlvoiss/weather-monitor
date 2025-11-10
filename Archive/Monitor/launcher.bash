#!/bin/bash
# launcher.bash
# navigate to Monitor, then execute weather and fan control python script

cd /home/pi/Monitor
date
echo "NOT Starting Weather & Fan Control Script"
python3 monitor.py
echo "Stopped Weather & Fan Control Script"
date
cp /home/pi/logs/weatherlog /home/pi/logs/weatherlog\.`date +%w`
cd

