from machine import Pin, I2C
import time
import struct

#I2Cの設定
bus = I2C(0, scl=Pin(33, pull=Pin.PULL_UP), sda=Pin(32, pull=Pin.PULL_UP), freq=400000, timeout=1000000)

#I2Cバス上に接続されているデバイスをスキャン
scan_res = bus.scan()
print([hex(x) for x in scan_res])

#Instruction set
POWER_DOWN = b'\x00'
POWER_ON   = b'\x01'
RESET      = b'\x07'
#Continuosly resolution mode
C_H_RESOLUTION_MODE  = b'\x10'
C_H_RESOLUTION_MODE2 = b'\x11'
C_L_RESOLUTION_MODE  = b'\x13'
#One time mode
ONE_TIME_H_RESOLUTION_MODE  = b'\x20'
ONE_TIME_H_RESOLUTION_MODE2 = b'\x21'
ONE_TIME_L_RESOLUTION_MODE  = b'\x23'

#アドレス
bh1750_i2c_L_addr = 0x23
bh1750_i2c_H_addr = 0X5c

#bh1750を起動後、reset
def bh1750_PowerOn(bus):
    poweron = b'\x01'
    bus.writeto(bh1750_i2c_L_addr, poweron)
    bus.writeto(bh1750_i2c_L_addr, RESET)

#照度を測るよう命令を送る
def bh1750_read_illuminance(bus):
    bus.writeto(bh1750_i2c_L_addr, C_H_RESOLUTION_MODE)
    result = bus.readfrom(bh1750_i2c_L_addr, 16)
    print(result)
    decimal_number = binary_to_decimal(result)
    illuminance_calculation(decimal_number)

def illuminance_calculation(result):
    illuminance = result / 1.2
    print(f"measured illuminunce: {illuminance} lx")

def binary_to_decimal(binary):
    decimal = 0
    power = len(binary) - 1
    for digit in binary:
        decimal += int(digit) * (2 ** power)
        power -= 1
    return decimal
 
bh1750_PowerOn(bus)
time.sleep(0.1)

while(True):
    bh1750_read_illuminance(bus)
    time.sleep(1.5)