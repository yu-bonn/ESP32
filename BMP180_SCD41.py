from machine import Pin, I2C
import time
import struct
import asyncio
import random
from umqtt.robust import MQTTClient
import network
import collections

work_queue : collections.deque[int] = collections.deque((), 16)
work_to_do : asyncio.Event = asyncio.Event()
work_to_do.clear()

#MQTTの設定
def net_setup() -> MQTTClient:
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    print(f"MAC: {wlan.config('mac').hex()}")
    res = wlan.connect('JAISTALL',"")
    print(f"connect result: {res}")

    mqtt_client:MQTTClient = MQTTClient(client_id = "2410139", server = "150.65.230.59")
    res = mqtt_client.connect()
    print(f"connect result: {res}")
    return mqtt_client

async def producer_scd41(co2:str, temp_num:str, h_num:str):
    num = 1
    while True:
        print("producer SCD41: queing work!")
        send_data = [num, co2, temp_num, h_num]
        work_queue.append(send_data)
        work_to_do.set()
        await asyncio.sleep(0.1)

async def producer_bmp180(temp_num:str, air_pressure):
    num = 2
    while True:
        print("producer BMP180: queing work!")
        send_data = [num, temp_num, air_pressure]
        work_queue.append(send_data)
        work_to_do.set()
        await asyncio.sleep(0.1)
        
def post_scd41_co2(data:int):
    client.publish("i483/sencer/2410139/SCD41/co2", f"{data}".encode())
    print("Publish SCD41_CO2")

def post_scd41_temp(data:int):
    client.publish("i483/sencer/2410139/SCD41/temp", f"{data}".encode())
    print("Publish SCD41_temp")

def post_scd41_h(data:int):
    client.publish("i483/sencer/2410139/SCD41/h", f"{data}".encode())
    print("Publish SCD41_h")

def post_bmp180_temp(data:int):
    client.publish("i483/sencer/2410139/BMP180/temp", f"{data}".encode())
    print("Publish BMP180_temp")

def post_bmp180_pressure(data:int):
    client.publish("i483/sencer/2410139/BMP180/pressure", f"{data}".encode())
    print("Publish BMP180_pressure")

async def consumer():
    while True:
        await work_to_do.wait()
        work:int = work_queue.popleft()
        print(f"consumer: consuming {work=}, items in queue: {len(work_queue)}")
        #stop ourselves from blocking in get
        if len(work_queue) == 0:
            work_to_do.clear()
        if work[0] == 1:
            work.pop(0)
            post_scd41_co2(work[0])
            post_scd41_temp(work[1])
            post_scd41_h(work[2])
        elif work[0] == 2:
            work.pop(0)
            post_bmp180_temp(work[0])
            post_bmp180_pressure(work[1])

#I2Cの設定
bus = I2C(0, scl=Pin(33, pull=Pin.PULL_UP), sda=Pin(32, pull=Pin.PULL_UP), freq=400000, timeout=1000000)

#I2Cバス上に接続されているデバイスをスキャン
scan_res = bus.scan()
print([hex(x) for x in scan_res])

#SCD41 I2C address
scd41_i2c_addr = 0x62

#SCD41_command
START_MEASUREMENT = bytearray([0x21, 0xb1])
READ_MEASUREMENT  = bytearray([0xec, 0x05])
STOP_MEASUREMENT  = bytearray([0x3f, 0x86])

SINGLE_MEASUREMENT_MODE = bytearray([0x21, 0x9d])
WAKE_UP = bytearray([0x36, 0xf6])
GET_DATA_READY_STATUS = bytearray([0xe4, 0xb8])

#SCD41_code
def scd41_PowerOn():
    bus.writeto(scd41_i2c_addr, STOP_MEASUREMENT)
    time.sleep(1)
    bus.writeto(scd41_i2c_addr, START_MEASUREMENT)
    time.sleep(1)
    
def scd41_WakeUp():
    bus.writeto(scd41_i2c_addr, WAKE_UP)
    time.sleep(1)

def scd41_get_data_ready():
    bus.writeto(scd41_i2c_addr, GET_DATA_READY_STATUS)
    time.sleep(1)
    result = bus.readfrom(scd41_i2c_addr, 3)
    result_num = int.from_bytes(result[:2],'big')
    return (result_num & 0x07ff) != 0

def scd41_measurement_signal():
    bus.writeto(scd41_i2c_addr, SINGLE_MEASUREMENT_MODE)
    time.sleep(1)
    
def scd41_read_measurement():
    bus.writeto(scd41_i2c_addr, READ_MEASUREMENT)
    data = bus.readfrom(scd41_i2c_addr, 9)
    return data

def scd41_parse_data(data):
    co2 = int.from_bytes(data[:2], 'big')
    temp= int.from_bytes(data[3:5], 'big')
    rh = int.from_bytes(data[6:8], 'big')
    
    temp_num = -45 + 175 * temp / ((2**16)-1)
    h_num =100 * rh / ((2**16)-1)

    print(f"SCD41: CO2: {co2} (ppm), temperature: {temp_num:.2f} C, humidity: {h_num:.2f} %")
    producer_scd41(co2, temp_num, h_num)

CRC8_POLYNOMIAL = 0x31
CRC8_INT = 0xff
def sdc41_crc(data):
    crc = CRC8_INT
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = (crc <<1) ^ CRC8_POLYNOMIAL
            else:
                crc <<= 1
            crc &= 0xFF
    return crc

def scd41_crc_correct(raw):
    for i in range(3):
        if sdc41_crc(raw[i*3:(i+1)*3]*1):
           print("SCD41: CRC ERROR at word number {}".i)
           return False
    return True

def scd41_poll():
    if not scd41_get_data_ready():
        print("SCD41: no new data available")
        return
    raw_measurement = scd41_read_measurement()
    if not scd41_crc_correct(raw_measurement):
        print("SCD41: crc error")
        return
    
    scd41_parse_data(raw_measurement)

#BMP180_code
bmp180_i2c_addr = 0x77

def bmp180_read_chip_id(bus):
    bmp180_reg_chip_id = b'\xd0'

    bus.writeto(bmp180_i2c_addr, bmp180_reg_chip_id, False)
    chip_id = bus.readfrom(bmp180_i2c_addr, 1)

    if chip_id[0] == 0x55:
        print("chip id is 0x55, BMP180 detected")
    else:
        print(f"chip id is: {hex(chip_id[0])}, NOT BMP180!")
        
def bmp180_read_coefficients(bus)-> Bytes:
    bmp180_coef_reg_base = b'\xaa'
    bmp180_coef_size = 22
    
    bus.writeto(bmp180_i2c_addr, bmp180_coef_reg_base, False)
    coefs = bus.readfrom(bmp180_i2c_addr, bmp180_coef_size)
    
    print(f"bmp coefficients: {coefs=}")
    return coefs

def bmp180_perform_measurement(bus, command: Bytes, ms: int) -> Bytes :
    bmp180_reg_out_msb = b'\xf6'
    
    bus.writeto(bmp180_i2c_addr, command, True)
    time.sleep_ms(ms)
    
    bus.writeto(bmp180_i2c_addr, bmp180_reg_out_msb, False)
    out = bus.readfrom(bmp180_i2c_addr, 3)
    
    print(f"raw output: {[hex(x) for x in out]}")
    return out    

def bmp180_read_temperature(bus)->int:
    bmp180_cmd_meas_temp = b'\xf4\x2e'
    
    return bmp180_perform_measurement(bus, bmp180_cmd_meas_temp, 5)

def bmp180_read_pressure(bus)->int:
    bmp180_cmd_meas_temp = b'\xf4\xf4'
    
    return bmp180_perform_measurement(bus, bmp180_cmd_meas_temp, 26)

def compute(coef, raw_temp, raw_press):
    #this is horrible, but it is what the spec sheet says you should do
    #first, let's parse our coefficients
    print("data computation")
    
    #int.from_bytes exists, but more limited to struct
    #UT = int.from_bytes(raw_temp, 'big', True)
    UT = struct.unpack_from(">h", raw_temp)[0]
    #Q what do we do with xlsb?
    #UP = struct.unpack_from(">h", raw_press)[0]
    #UP is.. special, time to shift things around
    oss = 3
    UP = raw_press[0] << 16 | raw_press[1] << 8 | raw_press[2]
    UP = UP >> (8 - oss)
    
    AC1 = struct.unpack_from(">h", coef)[0]
    AC2 = struct.unpack_from(">h", coef, 2)[0]
    AC3 = struct.unpack_from(">h", coef, 4)[0]
    AC4 = struct.unpack_from(">H", coef, 6)[0]
    AC5 = struct.unpack_from(">H", coef, 8)[0]
    AC6 = struct.unpack_from(">H", coef, 10)[0]
    B1 = struct.unpack_from(">h", coef, 12)[0]
    B2 = struct.unpack_from(">h", coef, 14)[0]
    MB = struct.unpack_from(">h", coef, 16)[0]
    MC = struct.unpack_from(">h", coef, 18)[0]
    MD = struct.unpack_from(">h", coef, 20)[0]
    
    print(f"{UT=}, {UP=}")
    print(f"{AC1=}, {AC2=}, {AC3=}, {AC4=}, {AC5=}, {AC6=}")
    print(f"{B1=}, {B2=}, {MB=}, {MC=}, {MD=}")

    #compute temperature
    X1 = (UT - AC6) * AC5 // 0x8000
    X2 = MC * 0x0800 // (X1 + MD)
    B5 = X1 + X2
    T = (B5 + 8) // 0x0010
    #T is in 0.1C units
    print(f"measured temperature: {T / 10} ")
    
    #compute pressure
    B6 = B5 - 4000
    X1 = (B2 * (B6 * B6 // (1 << 12))) // (1 < 11)
    X2 = AC2 * B6 // (1 << 11)
    X3 = X1 + X2
    B3 = (((AC1 * 4 + X3) << oss) + 2) // 4
    X1 = AC3 * B6 // (1 << 13)
    X2 = (B1 * (B6 * B6 // (1 << 12))) // (1 << 16)
    X3 = ((X1 + X2) + 2) // 4
    
    #unsigned longs here, check later
    B4 = AC4 * (X3 + 32768) // (1 << 15)
    B7 = (UP - B3) * (50000 >> 3)
    if B7 < 0x80000000 :
        p = (B7 * 2) // B4
    else:
        p = (B7 // B4) * 2
    X1 = (p // 256) * (p // 256)
    X1 = (X1 * 3038) // ( 1 << 16)
    X2 = (-7357 * p) // (1 << 16)
    p = p + (X1 + X2 + 3791) // 16
    
    print(f"measured air pressure: {p/100} hPa")
    producer_bmp180(T /10, p/100)

################################################

client:MQTTClient = net_setup()
asyncio.run(consumer())
scd41_PowerOn()
bmp180_read_chip_id(bus)
coef = bmp180_read_coefficients(bus)

while True:
    print("SCD41 start\n")
    scd41_poll()
    print("BMP180 start\n")
    raw_temp = bmp180_read_temperature(bus)
    raw_press = bmp180_read_pressure(bus)
    compute(coef, raw_temp, raw_press)
    time.sleep(15)
    print("15second\n")
    