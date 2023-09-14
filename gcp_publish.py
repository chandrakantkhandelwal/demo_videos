import random
from gcp_mqtt_client import get_client
from paho.mqtt import client as mqtt_client
import os
import logging
import time
import datetime
import json

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()

# local mqtt settings
local_broker = 'ia_mqtt_broker'
local_port = 1883

# GCP mqtt settings 
project_id=os.getenv("PROJECT")
cloud_region=os.getenv("CLOUD_REGION")
registry_id=os.getenv("REGISTRY_ID")
device_id=os.getenv("DEVICE_ID")
private_key_file="/gcp_certs/"
algorithm="RS256"
ca_certs="/gcp_certs/roots.pem"
mqtt_bridge_hostname=""
mqtt_bridge_port=8883            
# gcp_mqtt_topic = f"/devices/{device_id}/events"
gcp_mqtt_topic = "events"

jwt_exp_mins = 20
jwt_iat = datetime.datetime.now(tz=datetime.timezone.utc)

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(local_broker, local_port, 60)
    return client


def subscribe(client: mqtt_client, gcp_client):
    def on_message(client, userdata, msg):
        # logger.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        msg_dict = json.loads(str(msg.payload.decode("utf-8","ignore")))
        global jwt_iat
        seconds_since_issue = (
            datetime.datetime.now(tz=datetime.timezone.utc) - jwt_iat
        ).seconds
        # if seconds_since_issue > 60 * jwt_exp_mins:
        #     logger.info(f"Refreshing token after {seconds_since_issue}s")
        #     jwt_iat = datetime.datetime.now(tz=datetime.timezone.utc)
        #     gcp_client.loop()
        #     gcp_client.disconnect()
        #     gcp_client = get_client(
        #         project_id,
        #         cloud_region,
        #         registry_id,
        #         device_id,
        #         private_key_file,
        #         algorithm,
        #         ca_certs,
        #         mqtt_bridge_hostname,
        #         mqtt_bridge_port,
        #     )        
        # gcp_client.connect(mqtt_bridge_hostname, mqtt_bridge_port, 60)
        # gcp_client.loop_start()
        # gcp_client.publish(msg.topic, msg.payload.decode())
        # gcp_client.disconnect()
        # gcp_client.loop_stop()
        logger.info("GCP publish successful")

    client.subscribe(gcp_mqtt_topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    # gcp_client = get_client(
    #         project_id,
    #         cloud_region,
    #         registry_id,
    #         device_id,
    #         private_key_file,
    #         algorithm,
    #         ca_certs,
    #         mqtt_bridge_hostname,
    #         mqtt_bridge_port,
    #     )
    gcp_client = None
    logger.info("GCP client created")
    subscribe(client, gcp_client)
    client.loop_forever()

if __name__ == '__main__':
    run()
