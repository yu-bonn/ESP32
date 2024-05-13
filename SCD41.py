from machine import Pin, I2C
import time
import struct

#I2Cの設定
bus = I2C(0, scl=Pin(33, pull=Pin.PULL_UP), sda=Pin(32, pull=Pin.PULL_UP), freq=400000, timeout=1000000)

#I2Cバス上に接続されているデバイスをスキャン
scan_res = bus.scan()
print([hex(x) for x in scan_res])

#SCD41 I2C address
scd41_i2c_addr = 0x62

#command
START_MEASUREMENT = bytearray([0x21, 0xb1])
READ_MEASUREMENT  = bytearray([0xec, 0x05])
STOP_MEASUREMENT  = bytearray([0x3f, 0x86])

SINGLE_MEASUREMENT_MODE = bytearray([0x21, 0x9d])
WAKE_UP = bytearray([0x36, 0xf6])
GET_DATA_READY_STATUS = bytearray([0xe4, 0xb8])

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
    print(co2)
    
    temp_num = -45 + 175 * temp / ((2**16)-1)
    h_num =100 * rh / ((2**16)-1)
    
    print(f"SCD41: CO2: {co2} (ppm), temperature: {temp_num:.2f} C, humidity: {h_num:.2f} %")

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
    

scd41_PowerOn()
start_time = time.time()
while True:
    time.sleep(1)
    scd41_poll()
    if time.time() -start_time >= 15:
        break
print("15 seconds passed")