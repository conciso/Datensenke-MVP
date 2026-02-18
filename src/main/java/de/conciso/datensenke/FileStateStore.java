package de.conciso.datensenke;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class FileStateStore {

    private static final Logger log = LoggerFactory.getLogger(FileStateStore.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path stateFile;

    private final Map<String, FileStateEntry> fileState = new HashMap<>();
    private final Set<String> pendingDeleteIds = new HashSet<>();
    private final Map<String, PendingUpload> pendingUploads = new HashMap<>();

    public record FileStateEntry(String hash, long lastModified, String docId) {}

    public record PendingUpload(String fileName, String hash, Instant uploadedAt) {}

    public FileStateStore(@Value("${datensenke.state-file-path:data/datensenke-state.json}") String stateFilePath) {
        this.stateFile = Path.of(stateFilePath);
    }

    /**
     * Loads the persisted state from disk and returns it as a snapshot.
     * Does NOT modify the live fileState — call putEntry() to apply entries.
     */
    public Map<String, FileStateEntry> loadSnapshot() {
        if (!Files.exists(stateFile)) {
            log.info("No persisted state file found at {}", stateFile);
            return Map.of();
        }
        try {
            Map<String, FileStateEntry> state = objectMapper.readValue(
                    stateFile.toFile(),
                    new TypeReference<Map<String, FileStateEntry>>() {});
            log.info("Loaded persisted state: {} entries from {}", state.size(), stateFile);
            return state;
        } catch (Exception e) {
            log.warn("Failed to load state file: {}", e.getMessage());
            return Map.of();
        }
    }

    public void save() {
        try {
            Path parent = stateFile.getParent();
            if (parent != null) Files.createDirectories(parent);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(stateFile.toFile(), fileState);
        } catch (Exception e) {
            log.warn("Failed to save state file: {}", e.getMessage());
        }
    }

    // ── FileState ───────────────────────────────────────────────────────

    public FileStateEntry getEntry(String fileName) { return fileState.get(fileName); }

    public void putEntry(String fileName, FileStateEntry entry) { fileState.put(fileName, entry); }

    public void removeEntry(String fileName) { fileState.remove(fileName); }

    public Set<String> getFileNames() { return fileState.keySet(); }

    // ── Pending Deletes ─────────────────────────────────────────────────

    public Set<String> getPendingDeleteIds() { return pendingDeleteIds; }

    public void addPendingDelete(String docId) { pendingDeleteIds.add(docId); }

    // ── Pending Uploads ─────────────────────────────────────────────────

    public Map<String, PendingUpload> getPendingUploads() { return pendingUploads; }

    public void addPendingUpload(String trackId, PendingUpload upload) { pendingUploads.put(trackId, upload); }

    public void removePendingUpload(String trackId) { pendingUploads.remove(trackId); }
}
