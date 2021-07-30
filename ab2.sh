#!/bin/bash
 
while true; do
  docker-compose exec mids ab -n 2 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/sabre
  docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_shield/mirror_shield
  docker-compose exec mids ab -n 4 -H "Host: user1.comcast.com" http://localhost:5000/join_guild/house_stark
  docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_stark
  docker-compose exec mids ab -n 2 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/grubs
  docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/grubs
  docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/secret_club
  docker-compose exec mids ab -n 1 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/secret_club
  docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_greyjoy
  docker-compose exec mids ab -n 1 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/katana
  docker-compose exec mids ab -n 2 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/estoc
  docker-compose exec mids ab -n 1 -H "Host: user2.comcast.com" http://localhost:5000/purchase_a_shield/leather_shield
  sleep 10
done
