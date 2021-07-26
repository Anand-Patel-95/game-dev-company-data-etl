#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
events_topic = 'events'

tot_guild_member_cnt = 0   # count of total number of guild members
sword_names = {'copper_sword': 4, 'iron_sword' : 5, 'long_sword': 9, 'rapier': 7, 'short_sword': 5, 'master_sword': 77,
              'scimatar': 8, 'katana': 10, 'hook_sword': 4, 'machete': 5, 'falcion': 5, 'sabre': 6, 'great_sword': 15, 
              'moonlight_great_sword': 75, 'wood_sword': 1, 'estoc': 7}

shield_names = {'great_shield': 14, 'buckler' : 6, 'mirror_shield': 42, 'spiked_shield': 9, 'golden_shield': 40, 'wood_shield': 3,
              'leather_shield': 5, "copper_shield" : 4, "iron_shield": 7}

guilds_dict = {
    "house_stark" : {"member_cnt" : 0},
    "grubs" : {"member_cnt" : 0},
    "ucbears" : {"member_cnt" : 0},
    "joestars" : {"member_cnt" : 0},
    "winden_caves" : {"member_cnt" : 0}
}

def log_to_kafka(topic, event):
    event.update(request.headers) # gives us info coming from HTTP headers
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword/<sword_name>")
def purchase_a_sword(sword_name):
    # check if sword_name is in predefined dictionary
    sword_cost = sword_names.get(sword_name, None)
    
    purchase_sword_event = {'event_type': 'purchase_sword', 'type' : sword_name, 'cost': sword_cost}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased! " + sword_name +  "\n"


@app.route("/purchase_a_shield/<shield_name>")
def purchase_a_shield(shield_name):
    # check if shield_name is in predefined dictionary
    shield_cost = shield_names.get(shield_name, None)
    
    purchase_shield_event = {'event_type': 'purchase_shield', 'type' : shield_name, 'cost': shield_cost}
    log_to_kafka('events', purchase_shield_event)
    return "Shield Purchased! " + shield_name +  "\n"


@app.route("/join_guild/<guild_name>")
def join_guild(guild_name):
    # business logic to join guild
    global guilds_dict
    global tot_guild_member_cnt
    tot_guild_member_cnt += 1
    
    # check if guild name exists, otherwise create it
    if guild_name in guilds_dict:
        # increment the number of members in this guild
        guilds_dict[guild_name]["member_cnt"] += 1
    else:
        # create this guild, add this player to it
        guilds_dict[guild_name] = {"member_cnt" : 1}
    
    # log event to kafka    
    join_guild_event = {'event_type': 'join_guild', 'tot_guild_member_cnt': tot_guild_member_cnt, 'guild_name': guild_name, 'guild_member_cnt': guilds_dict[guild_name]["member_cnt"]}
    log_to_kafka('events', join_guild_event)
    
    return_str = "Joined Guild: " + guild_name + "! Total members:" + str(guilds_dict[guild_name]["member_cnt"]) + "\n"
    return return_str

@app.route("/leave_guild/<guild_name>")
def leave_guild(guild_name):
    # business logic to join guild
    global guilds_dict
    global tot_guild_member_cnt
    
    if guild_name in guilds_dict:
        # decrement the number of members in this guild
        guilds_dict[guild_name]["member_cnt"] -= 1
        if guilds_dict[guild_name]["member_cnt"] < 0:
            guilds_dict[guild_name]["member_cnt"] = 0
        
        ending_guild_mem_cnt = guilds_dict[guild_name]["member_cnt"]
        # decrement total guild member count
        tot_guild_member_cnt -= 1
        if tot_guild_member_cnt < 0:
            tot_guild_member_cnt = 0
        return_str = "Left Guild: "+ guild_name + "! Total members:" + str(ending_guild_mem_cnt) + "\n"
    else:
        # guild does not exist this. Do nothing
        return_str = "Can not leave Guild: "+ guild_name + "! This guild does not exist.\n"
        ending_guild_mem_cnt = None
        
    
    # log event to kafka    
    leave_guild_event = {'event_type': 'leave_guild', 'tot_guild_member_cnt': tot_guild_member_cnt, 'guild_name': guild_name, 'guild_member_cnt': ending_guild_mem_cnt}
    log_to_kafka('events', leave_guild_event)
        
    return return_str