# PortPollution-AISreduction
Read AIS messages from msgpack files and split them into different message types for various analysis

# To update dependencies.zip

    pip install -t dependencies -r requirements.txt
    cd dependencies
    zip -r ../dependencies.zip .
    
# To submit to Ymslanda:
    
    export DOCKER_HOST=tcp://ymslanda.innovationgarage.tech:2375
    docker stack rm portpollution_aisreduction
    sh deploy.sh
