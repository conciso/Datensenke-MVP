package de.conciso.datensenke;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class FailureLogWriter {

    private static final Logger log = LoggerFactory.getLogger(FailureLogWriter.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final int MAX_ROTATED_FILES = 5;

    private final Path logPath;
    private final long maxSizeBytes;

    public FailureLogWriter(
            @Value("${datensenke.failure-log-path:logs/datensenke-failures.log}") String failureLogPath,
            @Value("${datensenke.failure-log-max-size-kb:1024}") long maxSizeKb) {
        this.logPath = Path.of(failureLogPath);
        this.maxSizeBytes = maxSizeKb * 1024;
    }

    public void logFailure(String fileName, String reason, String trackId, String hash, String createdAt) {
        String timestamp = OffsetDateTime.now().format(TIMESTAMP_FORMAT);
        String line = String.format("%s | file=%s | reason=%s | track_id=%s | hash=%s | created_at=%s%n",
                timestamp,
                fileName != null ? fileName : "",
                reason != null ? reason : "",
                trackId != null ? trackId : "",
                hash != null ? hash : "",
                createdAt != null ? createdAt : "");
        try {
            Path parent = logPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            rotateIfNeeded();
            Files.writeString(logPath, line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write failure log entry for {}: {}", fileName, e.getMessage());
        }
    }

    public boolean isAlreadyLogged(String trackId, String createdAt) {
        if (trackId == null) {
            return false;
        }
        // Check current log and all rotated files
        for (int i = 0; i <= MAX_ROTATED_FILES; i++) {
            Path file = (i == 0) ? logPath : Path.of(logPath + "." + i);
            if (!Files.exists(file)) {
                continue;
            }
            try {
                for (String line : Files.readAllLines(file)) {
                    if (!line.contains("track_id=" + trackId)) {
                        continue;
                    }
                    if (createdAt == null || line.contains("created_at=" + createdAt)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                log.warn("Failed to read {} for dedup check: {}", file, e.getMessage());
            }
        }
        return false;
    }

    public boolean isFileHashFailed(String fileName, String hash) {
        if (fileName == null || hash == null) {
            return false;
        }
        for (int i = 0; i <= MAX_ROTATED_FILES; i++) {
            Path file = (i == 0) ? logPath : Path.of(logPath + "." + i);
            if (!Files.exists(file)) {
                continue;
            }
            try {
                for (String line : Files.readAllLines(file)) {
                    if (line.contains("file=" + fileName) && line.contains("hash=" + hash)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                log.warn("Failed to read {} for failed-file check: {}", file, e.getMessage());
            }
        }
        return false;
    }

    private void rotateIfNeeded() throws IOException {
        if (!Files.exists(logPath) || Files.size(logPath) < maxSizeBytes) {
            return;
        }
        log.info("Rotating failure log (size exceeded {} KB)", maxSizeBytes / 1024);
        // Shift existing rotated files: .4 → .5, .3 → .4, ...
        for (int i = MAX_ROTATED_FILES - 1; i >= 1; i--) {
            Path source = Path.of(logPath + "." + i);
            Path target = Path.of(logPath + "." + (i + 1));
            if (Files.exists(source)) {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        // Current log → .1
        Files.move(logPath, Path.of(logPath + ".1"), StandardCopyOption.REPLACE_EXISTING);
    }
}
