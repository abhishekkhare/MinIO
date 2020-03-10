package com.abhi.min.io.edu.min.io;

import io.minio.messages.Bucket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class MinioStorageController {
    @Autowired
    MinioAdapter minioAdapter;

    @GetMapping(path = "/buckets")
    public List<Bucket> listBuckets() {
        return minioAdapter.getAllBuckets();
    }

    @PostMapping(path = "/createbucket")
    public List<Bucket> createBucket(@NotNull @RequestBody String bucket) throws IOException {
        minioAdapter.createBucket(bucket);
        return minioAdapter.getAllBuckets();
    }

    @DeleteMapping(path = "/deletebucket")
    public List<Bucket> deleteBucket(@RequestParam(value = "bucket") String bucket) throws IOException {
        minioAdapter.deleteBucket(bucket);
        return minioAdapter.getAllBuckets();
    }


    @PostMapping(path = "/upload", consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    public Map<String, String> uploadFile(@RequestPart(value = "file", required = false) MultipartFile files) throws IOException {
        minioAdapter.uploadFile(files.getOriginalFilename(), files.getBytes(), null);
        Map<String, String> result = new HashMap<>();
        result.put("key", files.getOriginalFilename());
        return result;
    }

    @GetMapping(path = "/download")
    public ResponseEntity<ByteArrayResource> downloadFile(@RequestParam(value = "file") String file) throws IOException {
        byte[] data = minioAdapter.getFile(file, null);
        ByteArrayResource resource = new ByteArrayResource(data);

        return ResponseEntity
                .ok()
                .contentLength(data.length)
                .header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; filename=\"" + file + "\"")
                .body(resource);

    }

    @DeleteMapping(path = "{bucket}/delete")
    public void deleteFile(@PathVariable("bucket") final String bucket, @RequestParam(value = "file") String file) throws IOException {
        minioAdapter.delete(file, bucket);
    }

    @PostMapping(path = "{bucket}/upload", consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    public Map<String, String> uploadFile(@PathVariable("bucket") final String bucket, @RequestPart(value = "file", required = false) MultipartFile files) throws IOException {
        minioAdapter.uploadFile(files.getOriginalFilename(), files.getBytes(), bucket);
        Map<String, String> result = new HashMap<>();
        result.put("key", files.getOriginalFilename());
        return result;
    }

    @GetMapping(path = "{bucket}/download")
    public ResponseEntity<ByteArrayResource> downloadFile(@PathVariable("bucket") final String bucket, @RequestParam(value = "file") String file) throws IOException {
        byte[] data = minioAdapter.getFile(file, bucket);
        ByteArrayResource resource = new ByteArrayResource(data);

        return ResponseEntity
                .ok()
                .contentLength(data.length)
                .header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; filename=\"" + file + "\"")
                .body(resource);

    }

    @DeleteMapping(path = "/delete")
    public void deleteFile(@RequestParam(value = "file") String file) throws IOException {
        minioAdapter.delete(file, null);
    }
}