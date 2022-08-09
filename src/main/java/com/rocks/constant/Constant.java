package com.rocks.constant;

public class Constant {
    /** 分段迁移 超过该值采用分段 */
    public static final long MIGRATE_MAX_SIZE = 1024 * 1024 * 1024L;
    /** 分段迁移 每段大小 */
    public static final long MIGRATE_PART_SIZE = 200 * 1024 * 1024L;
    /** 分段迁移 上传超时时间: 单位秒 */
    public static final int MIGRATE_UPLOAD_TIMEOUT = 2 * 60 * 60;
    /** 分段下载和分段上传重试次数 */
    public static final int RETRY_COUNT = 5;
    /** 默认线程数 */
    public static final int THREAD_SIZE = 10;
    /** 默认返回失败文件数 */
    public static final int RETURN_FAILED_DOC_SIZE = 1000;
}
