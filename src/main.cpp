#include <Arduino.h>
#include <WiFi.h>
#include <AsyncTCP.h>
#include <ESPAsyncWebServer.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <ThreeWire.h>  
#include <RtcDS1302.h>
#include "time.h"

// ================= 硬件定义与多线程锁 =================
ThreeWire myWire(13, 14, 15); 
RtcDS1302<ThreeWire> Rtc(myWire);

const int PC1_SW = 26;
const int PC2_SW = 27;
const int PC1_LED = 32;
const int PC2_LED = 33;
unsigned long pc_pulse_end[3] = {0, 0, 0}; 

SemaphoreHandle_t modbusMutex;
SemaphoreHandle_t dataMutex;

// ================= 全局变量 =================
unsigned long last_read_time = 0;       
int current_task_idx = 0;             

String CONFIG_FILE = "/res/conf/config.jsonl";
String LOG_FILE = "/res/conf/logs.jsonl";
String HISTORY_FILE = "/res/conf/history.jsonl";
String MODBUS_DIM_FILE = "/res/conf/ModbusDim.jsonl"; 

struct AppConfig {
    String test_mode = "short";
    int test_limit = 50;
    String test_schedule = "none";
    int test_day = 1;
    int test_hour = 2;
    int test_minute = 0;
    bool buzzer_muted = true;
    int auto_start = 0;
    int log_limit = 50;
    int history_limit = 1440;
    String lang = "zh-cn";
    String sec_pwd = "";
    String ups_model_str = "--";
    String ups_version = "--";
    String ups_esn = "--";
} appConfig;

std::vector<std::pair<String, String>> wifi_list; 

// ================= 数据结构 =================
std::map<String, float> ups_data;
struct ReadTask { String key; uint16_t reg; float gain; uint8_t len; };
std::vector<ReadTask> read_tasks;
std::map<String, uint16_t> write_regs;

std::set<String> active_alarms;
unsigned long last_alarm_check = 0;
bool auto_deep_test_active = false;
bool manual_deep_test_active = false;
int last_test_trigger_minute = -1;
int last_bat_status = -1;
int last_history_min = -1;

int failed_modbus_reads = 10; 

AsyncWebServer server(80);

// ================= 安全鉴权助手函数 =================
bool checkSecurity(AsyncWebServerRequest *request) {
    // 如果没有设置密码，直接放行
    if (appConfig.sec_pwd == "" || appConfig.sec_pwd.length() == 0) return true;
    // 如果请求中带了正确的密码，放行
    if (request->hasParam("pwd") && request->getParam("pwd")->value() == appConfig.sec_pwd) return true;
    // 否则拦截
    return false;
}

// ================= 安全的 RTC 时钟读取 =================
RtcDateTime get_safe_time() {
    static RtcDateTime last_valid_time(2024, 1, 1, 0, 0, 0); 
    static unsigned long last_check = 0;
    unsigned long current_time = millis();

    if (last_check == 0 || current_time - last_check >= 1000) {
        last_check = current_time;
        RtcDateTime temp = Rtc.GetDateTime();
        if (temp.IsValid() && temp.Year() >= 2024) { last_valid_time = temp; }
    }
    return last_valid_time;
}

// ================= 文件操作 =================
void trim_file(String filepath, int limit, int buffer_size = 60) {
    File f = LittleFS.open(filepath, "r");
    if(!f) return;
    int count = 0;
    while(f.available()) { f.readStringUntil('\n'); count++; }
    f.close();

    if(count > limit + buffer_size) {
        f = LittleFS.open(filepath, "r");
        File fout = LittleFS.open(filepath + ".tmp", "w");
        int skip = count - limit;
        int idx = 0;
        while(f.available()) {
            String line = f.readStringUntil('\n');
            if(idx >= skip && line.length() > 0) fout.println(line);
            idx++;
        }
        f.close(); fout.close();
        LittleFS.remove(filepath);
        LittleFS.rename(filepath + ".tmp", filepath);
    }
}

void add_log(String type, String msg) {
    RtcDateTime now = get_safe_time();
    char time_str[30];
    snprintf(time_str, sizeof(time_str), "%04d-%02d-%02d %02d:%02d:%02d", 
             now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second());

    JsonDocument doc; doc["time"] = time_str; doc["type"] = type; doc["msg"] = msg;
    String out; serializeJson(doc, out);

    File f = LittleFS.open(LOG_FILE, "a");
    if(f) { f.println(out); f.close(); }
    trim_file(LOG_FILE, appConfig.log_limit, 10);
}

void save_config() {
    File f = LittleFS.open(CONFIG_FILE, "w");
    if(!f) return;
    
    f.printf("{\"test_mode\":\"%s\"}\n", appConfig.test_mode.c_str()); f.printf("{\"test_limit\":%d}\n", appConfig.test_limit);
    f.printf("{\"test_schedule\":\"%s\"}\n", appConfig.test_schedule.c_str()); f.printf("{\"test_day\":%d}\n", appConfig.test_day);
    f.printf("{\"test_hour\":%d}\n", appConfig.test_hour); f.printf("{\"test_minute\":%d}\n", appConfig.test_minute);
    f.printf("{\"buzzer_muted\":%s}\n", appConfig.buzzer_muted ? "true" : "false"); f.printf("{\"auto_start\":%d}\n", appConfig.auto_start);
    f.printf("{\"log_limit\":%d}\n", appConfig.log_limit); f.printf("{\"history_limit\":%d}\n", appConfig.history_limit);
    f.printf("{\"lang\":\"%s\"}\n", appConfig.lang.c_str()); f.printf("{\"sec_pwd\":\"%s\"}\n", appConfig.sec_pwd.c_str());
    f.printf("{\"ups_model_str\":\"%s\"}\n", appConfig.ups_model_str.c_str()); f.printf("{\"ups_version\":\"%s\"}\n", appConfig.ups_version.c_str());
    f.printf("{\"ups_esn\":\"%s\"}\n", appConfig.ups_esn.c_str());

    f.print("{\"wifi_list\":[");
    for(size_t i=0; i<wifi_list.size(); i++){ f.printf("{\"ssid\":\"%s\",\"pass\":\"%s\"}", wifi_list[i].first.c_str(), wifi_list[i].second.c_str()); if(i < wifi_list.size()-1) f.print(","); }
    f.print("]}\n"); f.close();
}

void load_config() {
    File f = LittleFS.open(CONFIG_FILE, "r");
    if(!f) { save_config(); return; }
    while(f.available()) {
        String line = f.readStringUntil('\n'); line.trim();
        if(line.length() == 0) continue;
        JsonDocument doc;
        if(deserializeJson(doc, line) == DeserializationError::Ok) {
            if(doc["wifi_list"].is<JsonArray>()) { JsonArray arr = doc["wifi_list"].as<JsonArray>(); wifi_list.clear(); for(JsonObject v : arr) wifi_list.push_back({v["ssid"].as<String>(), v["pass"].as<String>()}); }
            if(doc["test_mode"].is<const char*>()) appConfig.test_mode = doc["test_mode"].as<String>();
            if(doc["test_limit"].is<int>()) appConfig.test_limit = doc["test_limit"].as<int>();
            if(doc["test_schedule"].is<const char*>()) appConfig.test_schedule = doc["test_schedule"].as<String>();
            if(doc["test_day"].is<int>()) appConfig.test_day = doc["test_day"].as<int>();
            if(doc["test_hour"].is<int>()) appConfig.test_hour = doc["test_hour"].as<int>();
            if(doc["test_minute"].is<int>()) appConfig.test_minute = doc["test_minute"].as<int>();
            if(doc["sec_pwd"].is<const char*>()) appConfig.sec_pwd = doc["sec_pwd"].as<String>();
            if(doc["buzzer_muted"].is<bool>()) appConfig.buzzer_muted = doc["buzzer_muted"].as<bool>();
            if(doc["auto_start"].is<int>()) appConfig.auto_start = doc["auto_start"].as<int>();
            if(doc["log_limit"].is<int>()) appConfig.log_limit = doc["log_limit"].as<int>();
            if(doc["history_limit"].is<int>()) appConfig.history_limit = doc["history_limit"].as<int>();
            if(doc["lang"].is<const char*>()) appConfig.lang = doc["lang"].as<String>();
            if(doc["ups_model_str"].is<const char*>()) appConfig.ups_model_str = doc["ups_model_str"].as<String>();
            if(doc["ups_version"].is<const char*>()) appConfig.ups_version = doc["ups_version"].as<String>();
            if(doc["ups_esn"].is<const char*>()) appConfig.ups_esn = doc["ups_esn"].as<String>();
        }
    }
    f.close();
}

void init_modbus_dim() {
    File f = LittleFS.open(MODBUS_DIM_FILE, "r");
    if(!f) return;
    while(f.available()) {
        String line = f.readStringUntil('\n'); line.trim();
        if(line.length() == 0) continue;
        JsonDocument doc;
        if(deserializeJson(doc, line) == DeserializationError::Ok) {
            String type = doc["type"].as<String>(); String key = doc["key"].as<String>(); uint16_t reg = doc["reg"].as<uint16_t>();
            if(type == "read_task") { read_tasks.push_back({key, reg, doc["gain"].as<float>(), doc["len"].as<uint8_t>()}); ups_data[key] = 65535.0; } 
            else if(type == "write_reg") { write_regs[key] = reg; }
        }
    }
    f.close();
}

uint16_t calc_crc16(uint8_t *buf, uint8_t len) {
    uint16_t crc = 0xFFFF;
    for (int pos = 0; pos < len; pos++) { crc ^= (uint16_t)buf[pos]; for (int i = 8; i != 0; i--) { if ((crc & 0x0001) != 0) { crc >>= 1; crc ^= 0xA001; } else { crc >>= 1; } } }
    return crc;
}

uint32_t read_modbus_register(uint16_t reg_addr, uint8_t count) {
    while(Serial2.available()) { Serial2.read(); delay(1); }
    uint8_t frame[8] = {0x01, 0x03, (uint8_t)(reg_addr>>8), (uint8_t)(reg_addr&0xFF), 0x00, count};
    uint16_t crc = calc_crc16(frame, 6); frame[6] = crc & 0xFF; frame[7] = crc >> 8;
    
    Serial2.write(frame, 8); Serial2.flush(); delay(80);
    
    Serial2.setTimeout(200); uint8_t expected = 5 + count * 2; uint8_t buf[20]; int len = Serial2.readBytes(buf, expected);
    
    if(len == expected && buf[0] == 0x01 && buf[1] == 0x03) {
        uint16_t rx_crc = calc_crc16(buf, len - 2);
        if(buf[len-2] == (rx_crc & 0xFF) && buf[len-1] == (rx_crc >> 8)) {
            if(count == 1) return (buf[3] << 8) | buf[4];
            if(count == 2) return (buf[3] << 24) | (buf[4] << 16) | (buf[5] << 8) | buf[6];
        }
    }
    return 0xFFFFFFFF; 
}

void write_modbus_register(uint16_t reg_addr, uint16_t value) {
    if (xSemaphoreTake(modbusMutex, portMAX_DELAY)) {
        while(Serial2.available()) { Serial2.read(); delay(1); }
        uint8_t frame[8] = {0x01, 0x06, (uint8_t)(reg_addr>>8), (uint8_t)(reg_addr&0xFF), (uint8_t)(value>>8), (uint8_t)(value&0xFF)};
        uint16_t crc = calc_crc16(frame, 6); frame[6] = crc & 0xFF; frame[7] = crc >> 8;
        Serial2.write(frame, 8); Serial2.flush(); delay(100); xSemaphoreGive(modbusMutex);
    }
}

void fetch_device_info() {
    if (appConfig.ups_model_str != "--" && appConfig.ups_esn != "--" && appConfig.ups_model_str != "") return;
    
    if (xSemaphoreTake(modbusMutex, portMAX_DELAY)) {
        while(Serial2.available()) Serial2.read();
        uint8_t cmd[] = {0x01, 0x2B, 0x0E, 0x03, 0x87, 0x31, 0x75};
        Serial2.write(cmd, sizeof(cmd)); Serial2.flush();
        delay(500); 

        uint8_t buf[256]; 
        int len = Serial2.readBytes(buf, 256);
        
        if (len > 0) {
            int start_idx = -1;
            for (int i = 0; i < len - 1; i++) {
                if (buf[i] == '1' && buf[i+1] == '=') { start_idx = i; break; }
            }
            
            if (start_idx != -1) {
                String s = "";
                for (int i = start_idx; i < len; i++) {
                    if (buf[i] >= 32 && buf[i] <= 126) s += (char)buf[i];
                }
                
                bool updated = false;
                int start = 0;
                while (start < s.length()) {
                    int end = s.indexOf(';', start);
                    if (end == -1) end = s.length(); 
                    
                    String pair = s.substring(start, end);
                    int eq = pair.indexOf('=');
                    if (eq != -1) {
                        String k = pair.substring(0, eq); String v = pair.substring(eq + 1);
                        if (k == "1") { appConfig.ups_model_str = v; updated = true; }
                        if (k == "2") { appConfig.ups_version = v; updated = true; }
                        if (k == "4") { appConfig.ups_esn = v; updated = true; }
                    }
                    start = end + 1;
                }
                if (updated) {
                    save_config();
                    Serial.println("==> 成功从设备读取并保存了序列号信息！");
                }
            }
        }
        xSemaphoreGive(modbusMutex);
    }
}

void connect_wifi() {
    WiFi.disconnect(true); delay(500); WiFi.mode(WIFI_STA);
    if (wifi_list.empty()) {
        Serial.println("警告：配置文件中没有发现 WiFi 列表！");
        return;
    }

    for (auto const& w : wifi_list) {
        if (w.first.length() == 0) continue;
        Serial.printf("尝试连接 WiFi: %s ", w.first.c_str());
        WiFi.begin(w.first.c_str(), w.second.c_str());
        int timeout = 20; 
        while (WiFi.status() != WL_CONNECTED && timeout > 0) { delay(500); Serial.print("."); timeout--; }
        
        if (WiFi.status() == WL_CONNECTED) {
            Serial.println("\n\n=========================================================");
            Serial.print(">>> WiFi 连接成功! 你的网页访问地址是: http://");
            Serial.println(WiFi.localIP());
            Serial.println("=========================================================\n");
            
            configTime(8 * 3600, 0, "ntp.aliyun.com"); struct tm timeinfo;
            if (getLocalTime(&timeinfo, 5000)) {
                Rtc.SetDateTime(RtcDateTime(timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday, timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec));
                add_log("System", "NTP Sync Success");
                Serial.println("NTP 网络时钟同步成功！");
            }
            return;
        } else { 
            Serial.println(" 失败，切换下一个...");
            WiFi.disconnect(true); delay(500); 
        }
    }
}

void setup() {
    Serial.begin(115200); Serial2.begin(9600, SERIAL_8N1, 16, 17);
    modbusMutex = xSemaphoreCreateMutex(); dataMutex = xSemaphoreCreateMutex();
    pinMode(PC1_SW, OUTPUT); digitalWrite(PC1_SW, LOW); pinMode(PC2_SW, OUTPUT); digitalWrite(PC2_SW, LOW);
    pinMode(PC1_LED, INPUT_PULLUP); pinMode(PC2_LED, INPUT_PULLUP);

    Rtc.Begin(); LittleFS.begin(true);
    load_config(); init_modbus_dim(); fetch_device_info(); connect_wifi();

    server.on("/", HTTP_GET, [](AsyncWebServerRequest *request){ request->send(LittleFS, "/res/index.html", "text/html; charset=utf-8"); });
    server.on("/m", HTTP_GET, [](AsyncWebServerRequest *request){ request->send(LittleFS, "/res/mobile.html", "text/html; charset=utf-8"); });
    server.serveStatic("/res/", LittleFS, "/res/");

    server.on("/api/status", HTTP_GET, [](AsyncWebServerRequest *request){
        JsonDocument doc;
        xSemaphoreTake(dataMutex, portMAX_DELAY);
        for (auto const& item : ups_data) doc[item.first] = (item.second == 65535.0) ? 0 : item.second;
        xSemaphoreGive(dataMutex);

        doc["ups_model_str"] = appConfig.ups_model_str; doc["ups_version"] = appConfig.ups_version; doc["ups_esn"] = appConfig.ups_esn;
        doc["pc1_state"] = (digitalRead(PC1_LED) == LOW) ? 1 : 0; doc["pc2_state"] = (digitalRead(PC2_LED) == LOW) ? 1 : 0;
        doc["auto_deep_test_active"] = auto_deep_test_active; doc["manual_deep_test_active"] = manual_deep_test_active;
        doc["test_limit"] = appConfig.test_limit; doc["buzzer_muted"] = appConfig.buzzer_muted;
        doc["sys_ram_free"] = ESP.getFreeHeap(); doc["sys_ram_total"] = ESP.getHeapSize();
        doc["sys_flash_free"] = LittleFS.totalBytes() - LittleFS.usedBytes(); doc["sys_flash_total"] = LittleFS.totalBytes();
        
        RtcDateTime now_api = get_safe_time(); 
        char t_str[30]; snprintf(t_str, sizeof(t_str), "%04d-%02d-%02d %02d:%02d:%02d", now_api.Year(), now_api.Month(), now_api.Day(), now_api.Hour(), now_api.Minute(), now_api.Second());
        doc["sys_time"] = t_str;

        String response; serializeJson(doc, response); request->send(200, "application/json; charset=utf-8", response);
    });

    server.on("/api/history", HTTP_GET, [](AsyncWebServerRequest *request){
        std::shared_ptr<File> f = std::make_shared<File>(LittleFS.open(HISTORY_FILE, "r"));
        if(!(*f) || f->size() == 0) { request->send(200, "application/json", "[]"); return; }
        std::shared_ptr<bool> eof_sent = std::make_shared<bool>(false);
        AsyncWebServerResponse *response = request->beginChunkedResponse("application/json", [f, eof_sent](uint8_t *buffer, size_t maxLen, size_t index) -> size_t {
            if (*eof_sent) { f->close(); return 0; }
            size_t len = 0; if (index == 0) buffer[len++] = '[';
            while (len + 2 < maxLen && f->available()) {
                char c = f->read();
                if (c == '\n') { if (f->available()) buffer[len++] = ','; } else if (c != '\r') { buffer[len++] = c; }
            }
            if (!f->available()) { buffer[len++] = ']'; *eof_sent = true; }
            return len;
        });
        request->send(response);
    });

    server.on("/api/logs", HTTP_GET, [](AsyncWebServerRequest *request){
        File f = LittleFS.open(LOG_FILE, "r"); std::vector<String> lines;
        if(f) { while(f.available()) { String line = f.readStringUntil('\n'); line.trim(); if(line.length() > 0) { lines.push_back(line); if(lines.size() > 100) lines.erase(lines.begin()); } } f.close(); }
        String response; response.reserve(8192); response = "[";
        for(int i = (int)lines.size() - 1; i >= 0; i--) { response += lines[i]; if(i > 0) response += ","; }
        response += "]"; request->send(200, "application/json", response);
    });

    // 清空历史与日志记录 (增加鉴权)
    server.on("/api/history_clear", HTTP_GET, [](AsyncWebServerRequest *request){ 
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        LittleFS.remove(HISTORY_FILE); request->send(200, "text/plain", "History deleted! Please restart device."); 
    });
    server.on("/api/logs_clear", HTTP_GET, [](AsyncWebServerRequest *request){ 
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        LittleFS.remove(LOG_FILE); add_log("System", "Logs cleared by user"); request->send(200, "text/plain", "OK"); 
    });

    server.on("/api/get_config", HTTP_GET, [](AsyncWebServerRequest *request){
        JsonDocument doc; doc["test_mode"] = appConfig.test_mode; doc["test_limit"] = appConfig.test_limit; doc["test_schedule"] = appConfig.test_schedule;
        doc["test_day"] = appConfig.test_day; doc["test_hour"] = appConfig.test_hour; doc["test_minute"] = appConfig.test_minute; doc["buzzer_muted"] = appConfig.buzzer_muted; doc["auto_start"] = appConfig.auto_start; doc["log_limit"] = appConfig.log_limit;
        doc["history_limit"] = appConfig.history_limit; doc["lang"] = appConfig.lang; 
        
        // 【修复 1】：把 ****** 改为符合正则的 NoChange
        doc["sec_pwd"] = appConfig.sec_pwd == "" ? "" : "NoChange"; 
        
        String response; serializeJson(doc, response); request->send(200, "application/json", response);
    });

    server.on("/api/set_config", HTTP_GET, [](AsyncWebServerRequest *request){
        if (request->hasParam("mode")) appConfig.test_mode = request->getParam("mode")->value();
        if (request->hasParam("limit")) appConfig.test_limit = request->getParam("limit")->value().toInt();
        if (request->hasParam("log_limit")) appConfig.log_limit = request->getParam("log_limit")->value().toInt();
        if (request->hasParam("hist_limit")) appConfig.history_limit = request->getParam("hist_limit")->value().toInt();
        if (request->hasParam("lang")) appConfig.lang = request->getParam("lang")->value();
        if (request->hasParam("buzzer")) { appConfig.buzzer_muted = (request->getParam("buzzer")->value() == "1"); write_modbus_register(write_regs["buzzer_mute"], appConfig.buzzer_muted ? 1 : 0); }
        if (request->hasParam("autostart")) { appConfig.auto_start = request->getParam("autostart")->value().toInt(); write_modbus_register(write_regs["auto_start"], appConfig.auto_start); }
        if (request->hasParam("sch")) appConfig.test_schedule = request->getParam("sch")->value();
        if (request->hasParam("day")) appConfig.test_day = request->getParam("day")->value().toInt();
        if (request->hasParam("h")) appConfig.test_hour = request->getParam("h")->value().toInt();
        if (request->hasParam("m")) appConfig.test_minute = request->getParam("m")->value().toInt();
        if (request->hasParam("set_pwd")) { 
            String pwd = request->getParam("set_pwd")->value(); pwd.replace("%20", ""); pwd.trim(); 
            
            // 【修复 2】：对应的，如果前端发来的是 NoChange，就代表不修改旧密码
            if(pwd != "NoChange") appConfig.sec_pwd = pwd; 
        }
        save_config(); add_log("Control", "Settings updated"); request->send(200, "text/plain", "OK");
    });

    // 调试端口 (增加鉴权)
    server.on("/api/debug", HTTP_GET, [](AsyncWebServerRequest *request){
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        if(request->hasParam("fc") && request->hasParam("reg") && request->hasParam("val")) {
            int fc = request->getParam("fc")->value().toInt(); uint16_t reg = request->getParam("reg")->value().toInt(); uint16_t val = request->getParam("val")->value().toInt();
            std::vector<uint8_t> res_data; uint8_t frame[8] = {0x01, (uint8_t)fc, (uint8_t)(reg>>8), (uint8_t)(reg&0xFF), (uint8_t)(val>>8), (uint8_t)(val&0xFF)};
            if (xSemaphoreTake(modbusMutex, portMAX_DELAY)) {
                while(Serial2.available()) Serial2.read(); uint16_t crc = calc_crc16(frame, 6); frame[6] = crc & 0xFF; frame[7] = crc >> 8;
                Serial2.write(frame, 8); Serial2.flush(); delay(150); while(Serial2.available()) res_data.push_back(Serial2.read()); xSemaphoreGive(modbusMutex);
            }
            JsonDocument doc; JsonArray req_arr = doc["req"].to<JsonArray>(); for(int i=0; i<8; i++) req_arr.add(frame[i]);
            JsonArray res_arr = doc["res"].to<JsonArray>(); for(uint8_t b : res_data) res_arr.add(b); String response; serializeJson(doc, response); request->send(200, "application/json", response);
        } else { request->send(400, "text/plain", "Bad Request"); }
    });

    // Hex 调试端口 (增加鉴权)
    server.on("/api/hex_debug", HTTP_GET, [](AsyncWebServerRequest *request){
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        if(request->hasParam("data")) {
            String hex_str = request->getParam("data")->value(); hex_str.replace("%20", ""); hex_str.replace("+", ""); hex_str.replace(" ", ""); hex_str.trim(); std::vector<uint8_t> frame;
            for(unsigned int i=0; i<hex_str.length(); i+=2) { String byteString = hex_str.substring(i, i+2); frame.push_back((uint8_t) strtol(byteString.c_str(), NULL, 16)); }
            std::vector<uint8_t> res_data;
            if (xSemaphoreTake(modbusMutex, portMAX_DELAY)) {
                while(Serial2.available()) Serial2.read(); Serial2.write(frame.data(), frame.size()); Serial2.flush(); delay(200); while(Serial2.available()) res_data.push_back(Serial2.read()); xSemaphoreGive(modbusMutex);
            }
            JsonDocument doc; doc["req"] = hex_str; JsonArray res_arr = doc["res"].to<JsonArray>(); for(uint8_t b : res_data) res_arr.add(b); String response; serializeJson(doc, response); request->send(200, "application/json", response);
        } else { request->send(400, "text/plain", "Bad Request"); }
    });

    // UPS 电源及测试控制 (核心鉴权目标)
    server.on("/api/ctrl", HTTP_GET, [](AsyncWebServerRequest *request){
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        if(request->hasParam("action")) {
            String act = request->getParam("action")->value();
            if(act == "power_on") { write_modbus_register(write_regs["power_on"], 1); add_log("Control", "UPS Turn ON"); }
            else if(act == "power_off") { write_modbus_register(write_regs["power_off"], 1); add_log("Control", "UPS Turn OFF"); }
            else if(act == "test_short") { write_modbus_register(write_regs["test_short"], 1); add_log("Control", "Short Test Started"); }
            else if(act == "test_deep") { write_modbus_register(write_regs["test_deep"], 1); manual_deep_test_active = true; add_log("Control", "Deep Test Started"); }
            else if(act == "test_stop") { write_modbus_register(write_regs["test_stop"], 1); auto_deep_test_active = false; manual_deep_test_active = false; add_log("Control", "Test Stopped"); }
        }
        request->send(200, "text/plain", "OK");
    });

    // PC 继电器控制 (核心鉴权目标)
    server.on("/api/pc_ctrl", HTTP_GET, [](AsyncWebServerRequest *request){
        if (!checkSecurity(request)) { request->send(403, "text/plain", "Forbidden"); return; }
        int pc_id = request->hasParam("pc") ? request->getParam("pc")->value().toInt() : 1; String act = request->hasParam("action") ? request->getParam("action")->value() : "power"; int pin = (pc_id == 1) ? PC1_SW : PC2_SW;
        digitalWrite(pin, HIGH);
        if(act == "power") { pc_pulse_end[pc_id] = millis() + 500; add_log("Control", "PC Power Pulse"); } else if(act == "force") { pc_pulse_end[pc_id] = millis() + 10000; add_log("Control", "PC Force Off"); }
        request->send(200, "text/plain", "OK");
    });

    server.begin();
}

void loop() {
    unsigned long current_time = millis();
    RtcDateTime now = get_safe_time(); 

    static unsigned long last_info_fetch = 0;
    if ((appConfig.ups_model_str == "--" || appConfig.ups_model_str == "") && current_time - last_info_fetch > 15000) {
        last_info_fetch = current_time;
        fetch_device_info();
    }

    for(int i=1; i<=2; i++) { if(pc_pulse_end[i] > 0 && current_time >= pc_pulse_end[i]) { digitalWrite((i==1)?PC1_SW:PC2_SW, LOW); pc_pulse_end[i] = 0; } }

    if (current_time - last_read_time >= 150 && read_tasks.size() > 0) {
        if (xSemaphoreTake(modbusMutex, 0) == pdTRUE) {
            last_read_time = current_time;
            ReadTask task = read_tasks[current_task_idx];
            uint32_t val = read_modbus_register(task.reg, task.len);
            
            if(val != 0xFFFFFFFF) {
                if (failed_modbus_reads > 5) {
                    xSemaphoreGive(modbusMutex); 
                    if (write_regs.count("buzzer_mute")) write_modbus_register(write_regs["buzzer_mute"], appConfig.buzzer_muted ? 1 : 0);
                    if (write_regs.count("auto_start")) write_modbus_register(write_regs["auto_start"], appConfig.auto_start);
                    xSemaphoreTake(modbusMutex, portMAX_DELAY); 
                    
                    add_log("System", "UPS 通讯建立，已自动下发系统设置");
                    Serial.println("==> 检测到 UPS 连通，已将自动开机与蜂鸣器设置下发至 UPS");
                }
                failed_modbus_reads = 0; 

                xSemaphoreTake(dataMutex, portMAX_DELAY);
                ups_data[task.key] = val / task.gain;
                if(task.key == "bat_status") {
                    int status_val = (int)ups_data["bat_status"];
                    if(last_bat_status != -1 && status_val != last_bat_status) { if(status_val == 5) add_log("Discharge", "Discharging"); else if(last_bat_status == 5) add_log("Discharge", "Discharge ended"); }
                    last_bat_status = status_val;
                }
                xSemaphoreGive(dataMutex);

                if(task.key == "bat_remain_cap" && auto_deep_test_active) {
                    if((int)ups_data["bat_remain_cap"] <= appConfig.test_limit) { xSemaphoreGive(modbusMutex); write_modbus_register(write_regs["test_stop"], 1); xSemaphoreTake(modbusMutex, portMAX_DELAY); auto_deep_test_active = false; add_log("System", "Deep test limit reached, stopped."); }
                }
            } else {
                if (failed_modbus_reads < 100) failed_modbus_reads++;
            }
            
            current_task_idx = (current_task_idx + 1) % (int)read_tasks.size();
            xSemaphoreGive(modbusMutex);
        }
    }

    if (now.Minute() != last_history_min) {
        bool is_boot = (last_history_min == -1); 
        last_history_min = now.Minute();
        
        if (!is_boot && ups_data["temperature"] != 65535.0 && ups_data["bat_voltage"] != 65535.0) {
            char time_buf[10]; snprintf(time_buf, sizeof(time_buf), "%02d:%02d", now.Hour(), now.Minute());
            
            JsonDocument doc; JsonArray arr = doc.to<JsonArray>();
            xSemaphoreTake(dataMutex, portMAX_DELAY);
            arr.add(time_buf); 
            arr.add(round(ups_data["temperature"]*10.0)/10.0); arr.add(round(ups_data["bat_voltage"]*10.0)/10.0); arr.add((int)ups_data["bat_remain_cap"]); 
            arr.add(round(ups_data["in_voltage"]*10.0)/10.0); arr.add(round(ups_data["out_voltage"]*10.0)/10.0); arr.add(round(ups_data["out_current"]*10.0)/10.0); 
            arr.add(round(ups_data["out_active_power"]*100.0)/100.0); arr.add(round(ups_data["out_apparent_power"]*100.0)/100.0); arr.add(round(ups_data["load_ratio"]*10.0)/10.0);
            xSemaphoreGive(dataMutex);

            String out; serializeJson(doc, out);
            File f = LittleFS.open(HISTORY_FILE, "a"); if(f) { f.println(out); f.close(); }
            trim_file(HISTORY_FILE, appConfig.history_limit, 60);
        }

        bool should_run = false;
        if (appConfig.test_schedule == "weekly" && now.DayOfWeek() == appConfig.test_day) { if (now.Hour() == appConfig.test_hour && now.Minute() == appConfig.test_minute) should_run = true; } 
        else if (appConfig.test_schedule == "monthly" && now.Day() == appConfig.test_day) { if (now.Hour() == appConfig.test_hour && now.Minute() == appConfig.test_minute) should_run = true; }
        if (should_run && last_test_trigger_minute != now.Minute()) {
            last_test_trigger_minute = now.Minute();
            if (appConfig.test_mode == "short") { write_modbus_register(write_regs["test_short"], 1); add_log("Control", "Auto Test: Short"); }
            else if (appConfig.test_mode == "deep") { write_modbus_register(write_regs["test_deep"], 1); auto_deep_test_active = true; add_log("Control", "Auto Test: Deep"); }
        }
    }

    if (current_time - last_alarm_check > 2000) {
        last_alarm_check = current_time;
        xSemaphoreTake(dataMutex, portMAX_DELAY);
        int d_in = (int)ups_data["alm_in"]; int is_ac_fail = (d_in != 65535 && ((d_in >> 8) & 1)) ? 1 : 0;
        struct AlmDef { String key; int bit; String name; };
        AlmDef alms[] = { {"alm_temp", 3, "内部过温"}, {"alm_in", 8, "市电中断"}, {"alm_byp", 1, "旁路电压异常"}, {"alm_byp", 2, "旁路频率异常"}, {"alm_bat1", 3, "电池过压"}, {"alm_bat2", 1, "电池需维护"}, {"alm_bat2", 3, "电池低压"}, {"alm_bat3", 4, "电池未接"}, {"alm_out", 5, "输出过载"}, {"alm_inv", 5, "逆变器异常"}, {"alm_inv", 6, "逆变器异常"}, {"alm_inv", 7, "逆变器异常"} };
        for(auto a : alms) {
            int val = (int)ups_data[a.key]; if(val == 65535) continue;
            int is_active = (val >> a.bit) & 1; if(a.name.indexOf("旁路") != -1 && is_ac_fail) is_active = 0;
            String a_id = a.key + "_" + String(a.bit);
            if(is_active && active_alarms.find(a_id) == active_alarms.end()) { active_alarms.insert(a_id); add_log("System", "警告发生: " + a.name); } 
            else if(!is_active && active_alarms.find(a_id) != active_alarms.end()) { active_alarms.erase(a_id); add_log("System", "警告恢复: " + a.name); }
        }
        xSemaphoreGive(dataMutex);
    }
}