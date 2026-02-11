package de.conciso.datensenke;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FileWatcherService {

    private static final Logger log = LoggerFactory.getLogger(FileWatcherService.class);

    private final Path watchDirectory;
    private final LightRagClient lightRagClient;
    private final Map<String, Long> fileState = new HashMap<>();

    public FileWatcherService(
            @Value("${datensenke.watch-directory}") String watchDirectory,
            LightRagClient lightRagClient) {
        this.watchDirectory = Path.of(watchDirectory);
        this.lightRagClient = lightRagClient;
    }

    @Scheduled(fixedDelayString = "${datensenke.poll-interval-ms}")
    public void poll() {
        log.debug("Polling directory: {}", watchDirectory);

        if (!Files.isDirectory(watchDirectory)) {
            log.warn("Watch directory does not exist: {}", watchDirectory);
            return;
        }

        Map<String, Path> currentFiles = scanPdfFiles();

        handleNewAndUpdatedFiles(currentFiles);
        handleDeletedFiles(currentFiles);
    }

    private Map<String, Path> scanPdfFiles() {
        Map<String, Path> files = new HashMap<>();
        try (Stream<Path> stream = Files.list(watchDirectory)) {
            stream.filter(p -> p.toString().toLowerCase().endsWith(".pdf"))
                    .forEach(p -> files.put(p.getFileName().toString(), p));
        } catch (IOException e) {
            log.error("Failed to scan directory: {}", e.getMessage());
        }
        return files;
    }

    private void handleNewAndUpdatedFiles(Map<String, Path> currentFiles) {
        for (var entry : currentFiles.entrySet()) {
            String fileName = entry.getKey();
            Path filePath = entry.getValue();

            try {
                long lastModified = Files.getLastModifiedTime(filePath).toMillis();

                if (!fileState.containsKey(fileName)) {
                    log.info("CREATE: {}", fileName);
                    lightRagClient.uploadDocument(filePath);
                    fileState.put(fileName, lastModified);
                } else if (fileState.get(fileName) != lastModified) {
                    log.info("UPDATE: {} (delete + re-upload)", fileName);
                    deleteByFileName(fileName);
                    lightRagClient.uploadDocument(filePath);
                    fileState.put(fileName, lastModified);
                }
            } catch (Exception e) {
                log.error("Failed to process {}: {}", fileName, e.getMessage());
            }
        }
    }

    private void handleDeletedFiles(Map<String, Path> currentFiles) {
        var removedFiles = fileState.keySet().stream()
                .filter(name -> !currentFiles.containsKey(name))
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
