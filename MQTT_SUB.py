import asyncio
import random
import collections
from umqtt.robust import MQTTClient
import network
from machine import Pin

work_queue : collections.deque[int] = collections.deque((), 16)
work_to_do : asyncio.Event = asyncio.Event()
work_to_do.clear()

led = Pin(2, Pin.OUT)
led_state = False

def mqtt_callback(topic: str, msg: bytes):
    print(f"mqtt callback! {topic=}, msg: {msg.decode('UTF-8')}, binary data: {msg.hex()}")
    global led_state
    led_state = not led_state
    led.value(led_state)

def net_setup() -> MQTTClient:
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    print(f"MAC: {wlan.config('mac').hex()}")
    res = wlan.connect('JAISTALL',"")
    print(f"connect result: {res}")
    
    mqtt_client:MQTTClient = MQTTClient(client_id="2410139", server="150.65.230.59")
    res = mqtt_client.connect()
    print(f"connect result: {res}")
    mqtt_client.set_callback(mqtt_callback)
    res = mqtt_client.subscribe("i483/sencer/2410139/SCD41")
    print(f"subscribe result: {res}")
    return mqtt_client

client:MQTTClient = net_setup()

async def poll_mqtt():
    while True:
        #client.wait_msg()
        client.check_msg()
        await asyncio.sleep(0.1)
        print(f"polling!")

asyncio.run(poll_mqtt())