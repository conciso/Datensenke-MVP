package de.conciso.datensenke;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FileWatcherService {

    private static final Logger log = LoggerFactory.getLogger(FileWatcherService.class);

    private final RemoteFileSource remoteFileSource;
    private final LightRagClient lightRagClient;
    private final Map<String, Long> fileState = new HashMap<>();

    public FileWatcherService(RemoteFileSource remoteFileSource, LightRagClient lightRagClient) {
        this.remoteFileSource = remoteFileSource;
        this.lightRagClient = lightRagClient;
    }

    @Scheduled(fixedDelayString = "${datensenke.poll-interval-ms}")
    public void poll() {
        log.debug("Polling remote directory");

        List<RemoteFileInfo> currentFiles = remoteFileSource.listPdfFiles();
        Map<String, Long> currentFileMap = currentFiles.stream()
                .collect(Collectors.toMap(RemoteFileInfo::fileName, RemoteFileInfo::lastModified));

        handleNewAndUpdatedFiles(currentFileMap);
        handleDeletedFiles(currentFileMap.keySet());
    }

    private void handleNewAndUpdatedFiles(Map<String, Long> currentFiles) {
        for (var entry : currentFiles.entrySet()) {
            String fileName = entry.getKey();
            long lastModified = entry.getValue();

            try {
                if (!fileState.containsKey(fileName)) {
                    log.info("CREATE: {}", fileName);
                    downloadAndUpload(fileName);
                    fileState.put(fileName, lastModified);
                } else if (fileState.get(fileName) != lastModified) {
                    log.info("UPDATE: {} (delete + re-upload)", fileName);
                    deleteByFileName(fileName);
                    downloadAndUpload(fileName);
                    fileState.put(fileName, lastModified);
                }
            } catch (Exception e) {
                log.error("Failed to process {}: {}", fileName, e.getMessage());
            }
        }
    }

    private void downloadAndUpload(String fileName) {
        Path tempFile = remoteFileSource.downloadFile(fileName);
        try {
            lightRagClient.uploadDocument(tempFile);
        } finally {
            try {
                Files.deleteIfExists(tempFile);
            } catch (Exception e) {
                log.warn("Failed to delete temp file {}: {}", tempFile, e.getMessage());
            }
        }
    }

    private void handleDeletedFiles(Set<String> currentFileNames) {
        var removedFiles = fileState.keySet().stream()
                .filter(name -> !currentFileNames.contains(name))
                .toList();

        for (String fileName : removedFiles) {
            try {
                log.info("DELETE: {}", fileName);
                deleteByFileName(fileName);
                fileState.remove(fileName);
            } catch (Exception e) {
                log.error("Failed to delete {}: {}", fileName, e.getMessage());
            }
        }
    }

    private void deleteByFileName(String fileName) {
        var documents = lightRagClient.getDocuments();
        documents.stream()
                .filter(doc -> doc.file_path() != null && doc.file_path().endsWith(fileName))
                .findFirst()
                .ifPresentOrElse(
                        doc -> lightRagClient.deleteDocument(doc.id()),
                        () -> log.warn("Document not found in LightRAG for file: {}", fileName)
                );
    }
}
