package com.kk.mq.model;

/**
 * 登陆 message 3000万体验金活动 拉新活动
 */
public class LoginMessage {
    private int consumerId;
    private String phone;
    private String deviceType; // android,ios
    private String version; // 登录设备版本
    private String deviceId; // 设备Id
    private String loginTime; // yyyy-MM-dd HH:mm:ss


    public int getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(int consumerId) {
        this.consumerId = consumerId;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(String loginTime) {
        this.loginTime = loginTime;
    }
}
