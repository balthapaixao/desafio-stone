#! /bin/sh
#stop all contaners
sudo docker container stop $(sudo docker container ls –aq)
#kill all containers
sudo docker container rm $(sudo docker container ls –aq)

sudo docker rm -f /teste-postgres
sudo docker rm -f /teste-pgadmin
sudo docker rm -f teste-postgres
sudo docker rm -f teste-jupyter
#kill all unused networks
sudo docker network prune

#clean all used ports
sudo kill -9 $(sudo lsof -t -i:8080)
sudo kill -9 $(sudo lsof -t 0.0.0.0:8080)
sudo kill -9 $(sudo lsof -t -i:5432)
sudo kill -9 $(sudo lsof -t -i:15432)
sudo kill -9 $(sudo lsof -t -i:8888)
sudo kill -9 $(sudo lsof -t 0.0.0.0:8888)