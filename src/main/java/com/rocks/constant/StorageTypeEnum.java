package com.rocks.constant;

/**
 * 存储类型
 */
public enum StorageTypeEnum {
    MOSS("1", "moss对象存储端"),
    OSS("2", "oss对象存储端"),
    LOCAL("0", "本地存储端");

    private String name;
    private String index;

    StorageTypeEnum(String index, String name) {
        this.name = name;
        this.index = index;
    }

    public String getCode() {
        return index;
    }
}
