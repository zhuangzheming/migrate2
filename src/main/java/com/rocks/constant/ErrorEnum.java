package com.rocks.constant;

public enum ErrorEnum {
    ORIGINAL_PATH_ERROR("1401", "源路径错误"),
    TARGET_PATH_ERROR("1402", "目标路径错误"),
    ORIGINAL_TARGET_PATH_ERROR("1403", "源和目标路径错误");

    private String index;
    private String name;

    ErrorEnum(String index, String name) {
        this.index = index;
        this.name = name;
    }

    public String getCode() {
        return index;
    }
}
