import paho.mqtt.client as mqtt

# ブローカーの設定
MQTT_BROKER = "150.65.230.59"  # 使用する MQTT ブローカーのアドレス
MQTT_PORT = 1883                  # MQTT ポート（通常は 1883）
MQTT_TOPIC_SCD41_co2 = "i483/sensors/s2410139/SCD41/co2"         # サブスクライブするトピック
MQTT_TOPIC_SCD41_temp = "i483/sensors/s2410139/SCD41/temperature" 
MQTT_TOPIC_SCD41_h = "i483/sensors/s2410139/SCD41/humidity" 
MQTT_TOPIC_BMP180_temp = "i483/sensors/s2410139/BMP180/temperature" 
MQTT_TOPIC_BMP180_pressure = "i483/sensors/s2410139/BMP180/air_pressure" 


# メッセージを受信したときのコールバック関数
def on_message(client, userdata, msg):
    print(f"Received message '{msg.payload.decode()}' on topic '{msg.topic}' with QoS {msg.qos}")

# クライアントを作成
client = mqtt.Client()

# コールバック関数の設定
client.on_message = on_message

# ブローカーに接続
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# 指定したトピックをサブスクライブ
client.subscribe(MQTT_TOPIC_SCD41_co2, qos=0)
client.subscribe(MQTT_TOPIC_SCD41_temp, qos=0)
client.subscribe(MQTT_TOPIC_SCD41_h, qos=0)
client.subscribe(MQTT_TOPIC_BMP180_temp, qos=0)
client.subscribe(MQTT_TOPIC_BMP180_pressure, qos=0)

# メッセージの受信を開始
client.loop_forever()
