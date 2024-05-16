/* i2c - Simple example

   Simple I2C example that shows how to initialize I2C
   as well as reading and writing from and to registers for a sensor connected over I2C.

   The sensor used in this example is a MPU9250 inertial measurement unit.

   For other examples please check:
   https://github.com/espressif/esp-idf/tree/master/examples

   See README.md file to get detailed usage of this example.

   This example code is in the Public Domain (or CC0 licensed, at your option.)

   Unless required by applicable law or agreed to in writing, this
   software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
   CONDITIONS OF ANY KIND, either express or implied.
*/
#include <stdint.h>
#include <stdio.h>
#include <time.h>
//#include "esp-err.h"
//#include "esp-long.h"
#include "driver/i2c.h"
//#include "endian.h"
#include "freertos/task.h"
#include "freertos/FreeRTOS.h"
//#include "portmacro.h"
#include <endian.h>

static const char *TAG = "i2c-simple-example";

#define I2C_MASTER_SCL_IO           CONFIG_I2C_MASTER_SCL      /*!< GPIO number used for I2C master clock */
#define I2C_MASTER_SDA_IO           CONFIG_I2C_MASTER_SDA      /*!< GPIO number used for I2C master data  */
#define I2C_MASTER_NUM              0                          /*!< I2C master i2c port number, the number of i2c peripheral interfaces available will depend on the chip */
#define I2C_MASTER_FREQ_HZ          400000                     /*!< I2C master clock frequency */
#define I2C_MASTER_TX_BUF_DISABLE   0                          /*!< I2C master doesn't need buffer */
#define I2C_MASTER_RX_BUF_DISABLE   0                          /*!< I2C master doesn't need buffer */
#define I2C_MASTER_TIMEOUT_MS       1000

#define BMP180_I2C_ADDR                 0x77


static esp_err_t i2c_master_init(void)
{
    int i2c_master_port = I2C_MASTER_NUM;

    i2c_config_t conf = {
        .mode = I2C_MODE_MASTER,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_pullup_en = GPIO_PULLUP_ENABLE,
        .scl_pullup_en = GPIO_PULLUP_ENABLE,
        .master.clk_speed = I2C_MASTER_FREQ_HZ,
    };

    i2c_param_config(i2c_master_port, &conf);

    return i2c_driver_install(i2c_master_port, conf.mode, I2C_MASTER_RX_BUF_DISABLE, I2C_MASTER_TX_BUF_DISABLE, 0);
}


void bmp180_read_id(){
    uint8_t bmp180_reg_chip_id = 0xd0;
    TickType_t delay = pdMS_TO_TICKS(I2C_MASTER_TIMEOUT_MS);
    uint8_t chip_id = 0;
    i2c_master_write_read_device(I2C_MASTER_NUM, BMP180_I2C_ADDR, bmp180_reg_chip_id, sizeof(bmp180_reg_chip_id), chip_id, 1, delay);
    if(chip_id == 0x55){
        printf("chip id is 0x55, BMP180 detected");
    }else{
        printf("NOT BMP180!");
    }
}

uint8_t* bmp180_read_coefficients(){
    uint8_t* bmp180_coef_reg_base = 0xAA;
    int bmp180_coef_size = 22;
    uint8_t* coef = (uint8_t*)malloc(22); 
    TickType_t delay = pdMS_TO_TICKS(I2C_MASTER_TIMEOUT_MS);
    i2c_master_write_read_device(I2C_MASTER_NUM, BMP180_I2C_ADDR, bmp180_coef_reg_base, sizeof(bmp180_coef_reg_base), coef, bmp180_coef_size, delay);
    printf("bmp coefficients: %s\n", coef);
    return coef;
}

uint8_t* bmp180_measurement(uint16_t command, int ms){
    uint8_t bmp180_reg_out_msb = 0xF6;
    uint8_t* out = (uint8_t*)malloc(3);

    TickType_t delay = pdMS_TO_TICKS(I2C_MASTER_TIMEOUT_MS);
    i2c_master_write_to_device(I2C_MASTER_NUM, BMP180_I2C_ADDR, command, sizeof(command), delay);
    TickType_t delay_ms = pdMS_TO_TICKS(ms);
    vTaskDelay(delay_ms);

    i2c_master_write_read_device(I2C_MASTER_NUM, BMP180_I2C_ADDR, bmp180_reg_out_msb, sizeof(bmp180_reg_out_msb), out, 3, delay);
    printf("raw output: %s\n", out);
    return out;
}

uint8_t* bmp180_read_temp(){
    uint16_t bmp180_reg_out_msb = {0xF4, 0x2E};
    return bmp180_measurement(bmp180_reg_out_msb, 5);
}

uint8_t* bmp180_read_pres(){
    uint16_t bmp180_cmd_meas_temp = {0xF4, 0xF4};
    return bmp180_measurement(bmp180_cmd_meas_temp, 26);
}

void compute(uint8_t* coef, uint8_t* raw_temp, uint8_t* raw_press){
    printf("data computation\n");
    uint16_t UT = be16toh(*(uint16_t *)&raw_temp[0]);
    int oss = 3;
    uint32_t UP = (uint32_t)be16toh(*(uint16_t *)&raw_press[0]);
    UP = UP >> (8 - oss);

    int16_t AC1 = be16toh(*(int16_t *)&coef[0]);
    int16_t AC2 = be16toh(*(int16_t *)&coef[2]);
    int16_t AC3 = be16toh(*(int16_t *)&coef[4]);
    uint16_t AC4 = be16toh(*(uint16_t *)&coef[6]);
    uint16_t AC5 = be16toh(*(uint16_t *)&coef[8]);
    uint16_t AC6 = be16toh(*(uint16_t *)&coef[10]);
    int16_t B1 = be16toh(*(int16_t *)&coef[12]);
    int16_t B2 = be16toh(*(int16_t *)&coef[14]);
    int16_t MB = be16toh(*(int16_t *)&coef[16]);
    int16_t MC = be16toh(*(int16_t *)&coef[18]);
    int16_t MD = be16toh(*(int16_t *)&coef[20]);

    printf("UT: %d, UP: %ld\n", UT, UP);
    printf("AC1: %d, AC2: %d, AC3: %d, AC4: %u, AC5: %u, AC6: %u\n", AC1, AC2, AC3, AC4, AC5, AC6);
    printf("B1: %d, B2: %d, MB: %d, MC: %d, MD: %d\n", B1, B2, MB, MC, MD);

    //compute temperature
    int32_t X1 = (UT - AC6) * AC5; // 0x8000
    int32_t X2 = MC * 0x0800; // (X1 + MD)
    int32_t B5 = X1 + X2;
    int32_t T = (B5 + 8); // 0x0010
    printf("measured temperature: %f\n", (float)T/10);

    //compute pressure
    int32_t B6 = B5 - 4000;
    X1 = (B2 * ((B6 * B6) / (1 << 12))) / (1 << 11);
    X2 = (AC2 * B6 / (1 << 11));
    int32_t X3 = X1 + X2;
    int32_t B3 = (((AC1 * 4 + X3) << oss) + 2) / 4;
    X1 = AC3 * B6 / (1 << 13);
    X2 = (B1 * ((B6 * B6) / (1 << 12))) / (1 << 16);
    X3 = (X1 + X2 + 2) / 4;

    //unsigned longs here, check later
    uint32_t B4 = AC4 * (X3 + 32768) / (1 << 15);
    uint32_t B7 = (UP - B3) * (50000 >> 3);
    int p = 0;
    if(B7 < 0x80000000){
        p = (B7 * 2) / B4;
    }else{
        p = (B7 / B4) * 2;
    }
    X1 = (p / 256) *(p / 256);
    X1 = (X1 * 3038) / (1 << 16);
    X2 = (-7357 * p) / (1 << 16);
    p += (X1 + X2 + 3791) >> 4;
    printf("air pressure: %f hPa", (float)p / 100);
}

void app_main(void){
    printf("Entering app_main()\n");
    ESP_ERROR_CHECK(i2c_master_init());
    //ESP_LOGI(TAG, "I2C initialized successfully");

    bmp180_read_id();
    
    //time_t start_time = time(NULL);
    //time_t current_time;
    uint8_t* coef = bmp180_read_coefficients();
    while(1){
        //vTaskDelay(1000 / portTICK_PERIOD_MS);
        uint8_t* raw_temp = bmp180_read_temp();
        uint8_t* raw_press = bmp180_read_pres();
        compute(coef, raw_temp, raw_press);

        //current_time = time(NULL);
        /*if(current_time - start_time >= 15){
            break;
        }*/
        free(raw_temp);
        free(raw_press);
    }
    //this is now unreachable 
    ESP_ERROR_CHECK(i2c_driver_delete(I2C_MASTER_NUM));
    //ESP_LOGI(TAG, "I2C de-initialized successfully");

    //printf("15 seconds passed \n");

}
