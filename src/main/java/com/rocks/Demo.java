package com.rocks;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.rocks.constant.StorageTypeEnum;
import com.rocks.service.ClientConfig;
import com.rocks.service.ObjectService;
import com.rocks.service.oto.*;
import com.rocks.utils.CountMapCache;
import com.rocks.utils.SSHClient;
import com.rocks.vo.ClientEntity;
import com.rocks.vo.ClientEntityMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Demo {

    /**
     * 对象存储 迁移 对象存储
     *
     * @param protocolType MOSS协议
     * @param failedList   迁移失败的桶名，为了重新上传
     * @param uuid         事件标识
     * @param workNum      线程数
     * @param marker       listObjects遍历的参数，用于掉线异常恢复
     */
    public void moveObject(String originalAddress, String originalAk, String originalSk, String orgBucket,
                           String targetAddress, String targetAk, String targetSk, String targetBucket,
                           String protocolType, List<String> failedList, String uuid, String originalType,
                           String targetType, String workNum, String multipartOverSize, String multipartSize,
                           String marker, String originalDir, String targetDir) {
        int num;
        long migrateMaxSize;
        long migratePartSize;
        try {
            num = Integer.parseInt(workNum);
            migrateMaxSize = Long.parseLong(multipartOverSize)  * 1024 * 1024L;
            migratePartSize = Long.parseLong(multipartSize) * 1024 * 1024L;
            logger.info(uuid + " :moveObject()  workNum:" + workNum + " multipartOverSize:" + multipartOverSize + " multipartSize:" + multipartSize);
        } catch (NumberFormatException e) {
            logger.error(uuid + " :moveObject() param failed. workNum:" + workNum + " multipartOverSize:" + multipartOverSize + " multipartSize:" + multipartSize);
            return;
        }

        ObjectToObjectBase oto = null;
        Object orgClient = null, targetClient = null;

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSocketTimeout(360000);

        // moss 到 moss
        if (StorageTypeEnum.MOSS.getCode().equals(originalType) && StorageTypeEnum.MOSS.getCode().equals(targetType)) {
            orgClient = ClientConfig.getS3Client(originalAddress, originalAk, originalSk, false, protocolType);
            targetClient = ClientConfig.getS3Client(targetAddress, targetAk, targetSk, false, protocolType);
            oto = new MossToMoss();
            oto.uuidAmazonS3ClientMap.put(uuid, (AmazonS3Client) targetClient);
        }
        // moss 到 oss
        else if (StorageTypeEnum.MOSS.getCode().equals(originalType) && StorageTypeEnum.OSS.getCode().equals(targetType)) {
            orgClient = ClientConfig.getS3Client(originalAddress, originalAk, originalSk, false, protocolType);
            targetClient = new OSSClient(targetAddress, targetAk, targetSk);
            oto = new MossToOss();
        }
        // oss 到 moss
        else if (StorageTypeEnum.OSS.getCode().equals(originalType) && StorageTypeEnum.MOSS.getCode().equals(targetType)) {
            orgClient = new OSSClient(originalAddress, originalAk, originalSk);
            targetClient = ClientConfig.getS3Client(targetAddress, targetAk, targetSk, false, protocolType);
            oto = new OssToMoss();
            oto.uuidAmazonS3ClientMap.put(uuid, (AmazonS3Client) targetClient);
        }
        // oss 到 oss
        else if (StorageTypeEnum.OSS.getCode().equals(originalType) && StorageTypeEnum.OSS.getCode().equals(targetType)) {
            orgClient = new OSSClient(originalAddress, originalAk, originalSk, clientConfiguration);
            targetClient = new OSSClient(targetAddress, targetAk, targetSk, clientConfiguration);
            oto = new OssToOss();
        }
        if (oto == null || orgClient == null || targetClient == null) {
            logger.error(uuid + " :moveObject(): oto or orgClient, targetClient is null. originalType: " + originalType + " targetType: " + targetType);
            return;
        }
        ClientEntity clientEntity = new ClientEntity(originalAddress, originalAk, originalSk, protocolType, targetAddress, targetAk, targetSk, protocolType);
        ClientEntityMap.put(uuid, clientEntity);
        oto.setWorkNum(num);
        oto.setMigrateMaxSize(migrateMaxSize);
        oto.setMigratePartSize(migratePartSize);
        oto.setOrgClient(orgClient);
        oto.setTargetClient(targetClient);
        oto.start(orgBucket, targetBucket, failedList, marker, uuid, originalDir, targetDir);
    }
}

