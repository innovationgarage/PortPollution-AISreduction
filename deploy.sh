# #! /bin/sh

# export DOCKER_HOST=tcp://ymslanda.innovationgarage.tech:2375

# docker build --tag ymslanda.innovationgarage.tech:5000/portpollution_aisreduction:latest .
# docker push ymslanda.innovationgarage.tech:5000/portpollution_aisreduction:latest
# docker stack deploy -c docker-compose.yml portpollution_aisreduction

DATA="."
spark-submit --master spark://ymslanda.innovationgarage.tech:7077 easyPi.py 100 $DATA/pi.res
