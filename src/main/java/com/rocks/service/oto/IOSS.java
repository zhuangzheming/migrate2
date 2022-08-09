package com.rocks.service.oto;

import java.io.InputStream;
import java.util.List;

/**
 * 基础操作
 * @author zhuang
 * @date 2021-10-15
 * @param <O> 源端服务
 * @param <T> 目的端服务
 * @param <M> ObjectMetadata 元数据信息
 * @param <B> S3Object/OOSObject等
 * @param <P> PartETag
 */
public interface IOSS<O, T, M, B, P> {
    /** 桶的对象总数 */
    int getObjectCount(O orgClient, String orgBucket, String dir);

    /** 桶是否存在  */
    boolean bucketExist(T client, String bucket);

    M headObject(O client, String bucket, String key);

    /** 普通下载 */
    B getObject(O client, String bucket, String key);

    /**
     * 分段迁移的分段下载
     * @param client 源端存储服务
     * @param bucket 桶名
     * @param key 对象名
     * @param start 下载对象的起始位置
     * @param end 下载对象的结束位置
     * @param uploadId 判断该分段任务是否结束
     * @return 存储对象
     */
    B multiPartDownloadRange(O client, String bucket, String key, long start, long end, String uploadId);

    /**
     * 分段迁移的分段上传
     * @param client 目的端存储服务
     * @param bucketName 桶名
     * @param key 对象名
     * @param inputStream 上传流
     * @param uploadId 分段上传的唯一标识
     * @param curPartSize 当前分段大小
     * @param partNumber 第几个分段
     * @param partETagList 分段编号和 ETags的列表，用于 completeMultipartUpload方法（完成分段上传方法）
     */
    void multiPartUpload(T client, String bucketName, String key,
                                InputStream inputStream, String uploadId, List<P> partETagList,
                                long curPartSize, int partNumber);
    /** 获取对象大小  */
    Long getObjectContentLength(O client, String bucket, String key) throws Exception;

    /**
     * 普通上传
     */
    void putObject(T client, String bucket, String obj, B object);

    /**
     * 普通上传
     */
    void putObject(T client, String bucket, String obj, InputStream input, M metadata);

}
