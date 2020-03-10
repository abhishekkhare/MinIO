package com.abhi.min.io.edu.min.io;

import io.minio.MinioClient;
import io.minio.messages.Bucket;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;

@Service
public class MinioAdapter {

    @Autowired
    MinioClient minioClient;

    @Value("${minio.buckek.name}")
    String defaultBucketName;

    @Value("${minio.default.folder}")
    String defaultBaseFolder;

    public List<Bucket> getAllBuckets() {
        try {
            return minioClient.listBuckets();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }


    public void uploadFile(String name, byte[] content, String bucketName) {
        File file = new File("/tmp/" + name);
        file.canWrite();
        file.canRead();
        try {
            FileOutputStream iofs = new FileOutputStream(file);
            iofs.write(content);
            if (null == bucketName) {
                if (!minioClient.bucketExists(defaultBucketName)) {
                    minioClient.makeBucket(defaultBucketName);
                }
                minioClient.putObject(defaultBucketName, defaultBaseFolder + name, file.getAbsolutePath());
            } else {
                if (!minioClient.bucketExists(bucketName)) {
                    minioClient.makeBucket(bucketName);
                }
                minioClient.putObject(bucketName, defaultBaseFolder + name, file.getAbsolutePath());
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public byte[] getFile(String key, String bucketName) {
        try {

            InputStream obj = null;
            if (null == bucketName) {
                obj = minioClient.getObject(defaultBucketName, defaultBaseFolder + "/" + key);
            } else {
                obj = minioClient.getObject(bucketName, defaultBaseFolder + "/" + key);
            }

            byte[] content = IOUtils.toByteArray(obj);
            obj.close();
            return content;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @PostConstruct
    public void init() {
    }

    public void createBucket(@NotNull String bucket) {
        try {
            minioClient.makeBucket(bucket);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public void delete(String file, String bucketName) {
        try {
            if (null == bucketName) {
                minioClient.removeObject(defaultBucketName, file);
            } else {
                minioClient.removeObject(bucketName, file);
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void deleteBucket(String bucket) {
        try {
            minioClient.removeBucket(bucket);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}