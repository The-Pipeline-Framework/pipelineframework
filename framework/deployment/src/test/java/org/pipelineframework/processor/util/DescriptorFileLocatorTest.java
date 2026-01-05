package org.pipelineframework.processor.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class DescriptorFileLocatorTest {

    private static final String PROTOBUF_DESCRIPTOR_OPTION = "protobuf.descriptor.path";
    private static final String PROTOBUF_DESCRIPTOR_FILE = "protobuf.descriptor.file";

    @TempDir
    Path tempDir;

    private DescriptorFileLocator locator;
    private String originalUserDir;
    private String originalMultiModuleDir;

    @BeforeEach
    void setUp() {
        locator = new DescriptorFileLocator();
        originalUserDir = System.getProperty("user.dir");
        originalMultiModuleDir = System.getProperty("maven.multiModuleProjectDirectory");
        System.setProperty("user.dir", tempDir.toString());
        System.clearProperty("maven.multiModuleProjectDirectory");
    }

    @AfterEach
    void tearDown() {
        if (originalUserDir != null) {
            System.setProperty("user.dir", originalUserDir);
        }
        if (originalMultiModuleDir != null) {
            System.setProperty("maven.multiModuleProjectDirectory", originalMultiModuleDir);
        } else {
            System.clearProperty("maven.multiModuleProjectDirectory");
        }
    }

    @Test
    void testLocateAndLoadDescriptorsFromCustomPath() throws IOException {
        // Create a mock descriptor file
        Path descriptorDir = tempDir.resolve("custom-descriptors");
        Files.createDirectories(descriptorDir);
        
        Path descriptorFile = descriptorDir.resolve("descriptor_set.dsc");
        
        // Create a simple FileDescriptorSet for testing
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("TestMessage"))
                .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                    .setName("TestService")
                    .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("remoteProcess")
                        .setInputType("TestMessage")
                        .setOutputType("TestMessage"))))
            .build();
        
        try (FileOutputStream fos = new FileOutputStream(descriptorFile.toFile())) {
            descriptorSet.writeTo(fos);
        }
        
        // Create processor options with custom path
        java.util.Map<String, String> processorOptions = java.util.Map.of(PROTOBUF_DESCRIPTOR_OPTION, descriptorDir.toString());
        
        // Test that the descriptor is found and loaded
        DescriptorProtos.FileDescriptorSet loadedSet = locator.locateAndLoadDescriptors(processorOptions);
        
        assertNotNull(loadedSet);
        assertEquals(1, loadedSet.getFileCount());
        assertEquals("test.proto", loadedSet.getFile(0).getName());
    }

    @Test
    void testLocateAndLoadDescriptorsFromSpecificFile() throws IOException {
        // Create a mock descriptor file
        Path descriptorFile = tempDir.resolve("specific.desc");
        
        // Create a simple FileDescriptorSet for testing
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("TestMessage"))
                .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                    .setName("TestService")
                    .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("remoteProcess")
                        .setInputType("TestMessage")
                        .setOutputType("TestMessage"))))
            .build();
        
        try (FileOutputStream fos = new FileOutputStream(descriptorFile.toFile())) {
            descriptorSet.writeTo(fos);
        }
        
        // Create processor options with specific file
        java.util.Map<String, String> processorOptions = java.util.Map.of(PROTOBUF_DESCRIPTOR_FILE, descriptorFile.toString());
        
        // Test that the descriptor is found and loaded
        DescriptorProtos.FileDescriptorSet loadedSet = locator.locateAndLoadDescriptors(processorOptions);
        
        assertNotNull(loadedSet);
        assertEquals(1, loadedSet.getFileCount());
        assertEquals("test.proto", loadedSet.getFile(0).getName());
    }

    @Test
    void testLocateAndLoadDescriptorsWhenFileDoesNotExist() {
        // Create processor options with non-existent file
        java.util.Map<String, String> processorOptions = java.util.Map.of(PROTOBUF_DESCRIPTOR_FILE, "/non/existent/file.desc");
        
        // Test that IOException is thrown
        assertThrows(IOException.class, () -> {
            locator.locateAndLoadDescriptors(processorOptions);
        });
    }

    @Test
    void testLocateAndLoadDescriptorsWhenNoDescriptorsFound() throws IOException {
        // Create processor options with empty list
        java.util.Map<String, String> processorOptions = Collections.emptyMap();
        
        // Create a directory structure that doesn't contain descriptor files
        Path nonDescriptorDir = tempDir.resolve("non-descriptor-dir");
        Files.createDirectories(nonDescriptorDir);
        
        // Create a non-descriptor file
        Path nonDescriptorFile = nonDescriptorDir.resolve("not-a-descriptor.txt");
        Files.write(nonDescriptorFile, "not a descriptor".getBytes());
        
        // Test that IOException is thrown when no descriptors are found
        assertThrows(IOException.class, () -> {
            locator.locateAndLoadDescriptors(processorOptions);
        });
    }

    @Test
    void testLocateAndLoadDescriptorsFromDefaultModule() throws IOException {
        Path rootDir = tempDir.resolve("root");
        Path serviceDir = rootDir.resolve("payment-status-svc");
        Path commonDir = rootDir.resolve("common");
        Files.createDirectories(serviceDir);
        Files.createDirectories(commonDir.resolve("target/generated-sources/grpc"));

        Path descriptorFile = commonDir.resolve("target/generated-sources/grpc/descriptor_set.dsc");
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder().setName("test.proto"))
            .build();

        try (FileOutputStream fos = new FileOutputStream(descriptorFile.toFile())) {
            descriptorSet.writeTo(fos);
        }

        System.setProperty("user.dir", serviceDir.toString());
        java.util.Map<String, String> processorOptions = Collections.emptyMap();

        DescriptorProtos.FileDescriptorSet loadedSet = locator.locateAndLoadDescriptors(processorOptions);
        assertNotNull(loadedSet);
        assertEquals(1, loadedSet.getFileCount());
        assertEquals("test.proto", loadedSet.getFile(0).getName());
    }

    @Test
    void selectsDescriptorSetContainingExpectedService() throws IOException {
        Path rootDir = tempDir.resolve("root");
        Path serviceDir = rootDir.resolve("crawl-source-svc");
        Path commonDir = rootDir.resolve("common");
        Path otherDir = tempDir.resolve("other-project");
        Path otherCommonDir = otherDir.resolve("common");

        Files.createDirectories(serviceDir);
        Files.createDirectories(commonDir.resolve("target/generated-sources/grpc"));
        Files.createDirectories(otherCommonDir.resolve("target/generated-sources/grpc"));

        writeDescriptorSet(
            commonDir.resolve("target/generated-sources/grpc/descriptor_set.dsc"),
            "other.proto",
            "OtherService");
        writeDescriptorSet(
            otherCommonDir.resolve("target/generated-sources/grpc/descriptor_set.dsc"),
            "target.proto",
            "TargetService");

        System.setProperty("user.dir", serviceDir.toString());
        System.setProperty("maven.multiModuleProjectDirectory", tempDir.toString());
        DescriptorProtos.FileDescriptorSet loadedSet = locator.locateAndLoadDescriptors(
            Collections.emptyMap(),
            Set.of("TargetService"));

        assertNotNull(loadedSet);
        assertEquals("target.proto", loadedSet.getFile(0).getName());
        assertEquals("TargetService", loadedSet.getFile(0).getService(0).getName());
    }

    private void writeDescriptorSet(Path descriptorFile, String fileName, String serviceName) throws IOException {
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(fileName)
                .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                    .setName(serviceName)
                    .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("remoteProcess")
                        .setInputType("TestMessage")
                        .setOutputType("TestMessage"))))
            .build();

        try (FileOutputStream fos = new FileOutputStream(descriptorFile.toFile())) {
            descriptorSet.writeTo(fos);
        }
    }
}
