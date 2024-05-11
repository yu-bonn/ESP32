from machine import Pin, I2C
import time
import struct

#I2Cの設定
bus = I2C(0, scl=Pin(33, pull=Pin.PULL_UP), sda=Pin(32, pull=Pin.PULL_UP), freq=400000, timeout=1000000)

#I2Cバス上に接続されているデバイスをスキャン
scan_res = bus.scan()
print([hex(x) for x in scan_res])

#Instruction set
POWER_DOWN = 0x00 
POWER_ON   = 0x01
RESET      = 0x07
#Continuosly resolution mode
C_H_RESOLUTION_MODE  = 0x10
C_H_RESOLUTION_MODE2 = 0x11
C_L_RESOLUTION_MODE  = 0x13
#One time mode
ONE_TIME_H_RESOLUTION_MODE  = 0x20
ONE_TIME_H_RESOLUTION_MODE2 = 0x21
ONE_TIME_L_RESOLUTION_MODE  = 0x23

#アドレス
bh1750_i2c_L_addr = 0x23
bh1750_i2c_H_addr = 0X5c

#bh1750を起動後、reset
def bh1750_PowerOn(bus):
    bus.writeto(bh1750_i2c_L_addr, POWER_ON)
    bus.writeto(bh1750_i2c_L_addr, RESET)

#照度を測るよう命令を送る
def bh1750_read_illuminance(bus):
    bus.writeto(bh1750_i2c_L_addr, C_H_RESOLUTION_MODE)
    result = bus.readfrom(bh1750_i2c_L_addr, 16)
    print(result)
    illuminance_calculation(result)

def illuminance_calculation(result):
    illuminance = result / 1.2
    print(illuminance)
 
bh1750_PowerOn(bus)
time.sleep(0.1)
bh1750_read_illuminance(bus)