package com.rocks.service.oto;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class MossObjectService implements IOSS<AmazonS3, AmazonS3, ObjectMetadata, S3Object, PartETag> {
    private static final Logger logger = Logger.getLogger("MossObjectService.class");

    @Override
    public int getObjectCount(AmazonS3 orgClient, String orgBucket, String dir) {
        boolean isTruncated = true;
        int totalNum = 0;
        String countNextMarker = "";
        List<S3ObjectSummary> summaryList;
        ListObjectsRequest request = new ListObjectsRequest();
        request.setEncodingType("null");
        request.setBucketName(orgBucket);
        request.setPrefix(dir);
        while (isTruncated) {
            try {
                request.setMarker(countNextMarker);
                ObjectListing objectListing = orgClient.listObjects(request);
                countNextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                summaryList = summaryList.stream().filter(s -> !s.getKey().equals(dir)).collect(Collectors.toList());
                totalNum += summaryList.size();
                isTruncated = objectListing.isTruncated();
            } catch (SdkClientException e) {
                logger.error("listObjectsCountLoop error.", e);
            }
        }
        return totalNum;
    }

    @Override
    public boolean bucketExist(AmazonS3 client, String bucket) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucket);
        try {
           client.headBucket(headBucketRequest);
        } catch (SdkClientException e) {
            return false;
        }
        return true;
    }

    @Override
    public ObjectMetadata headObject(AmazonS3 client, String bucket, String key) {
        return client.getObjectMetadata(bucket, key);
    }

    @Override
    public S3Object getObject(AmazonS3 client, String bucket, String key) {
        return client.getObject(bucket, key);
    }

    @Override
    public S3Object multiPartDownloadRange(AmazonS3 client, String bucket, String key, long start, long end, String uploadId) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
        getObjectRequest.setRange(start, end);
        return client.getObject(getObjectRequest);
    }

    @Override
    public Long getObjectContentLength(AmazonS3 client, String bucket, String key) throws SdkClientException {
        ObjectMetadata objectMetadata;
        try {
            objectMetadata = client.getObjectMetadata(bucket, key);
        } catch (SdkClientException e) {
            throw new SdkClientException(e.getMessage());
        }
        return objectMetadata.getContentLength();
    }

    @Override
    public void putObject(AmazonS3 client, String bucket, String obj, S3Object object) {
        putObject(client, bucket, obj, object.getObjectContent(), object.getObjectMetadata());
    }

    /**
     * 上传对象
     */
    @Override
    public void putObject(AmazonS3 client, String bucket, String obj, InputStream input, ObjectMetadata metadata) {
        PutObjectRequest request = new PutObjectRequest(bucket, obj, input, metadata);
        client.putObject(request);
    }

    /**
     * 分段迁移的分段上传
     */
    @Override
    public void multiPartUpload(AmazonS3 client, String bucketName, String obj,
                                InputStream inputStream, String uploadId, List<PartETag> partETagList,
                                long curPartSize, int partNumber) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.setKey(obj);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartSize(curPartSize);
        uploadPartRequest.setPartNumber(partNumber);
        UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
        partETagList.add(uploadPartResult.getPartETag());
    }

}
