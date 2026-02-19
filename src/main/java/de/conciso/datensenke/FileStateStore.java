package de.conciso.datensenke;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class FileStateStore {

    private static final Logger log = LoggerFactory.getLogger(FileStateStore.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path stateFile;

    private final Map<String, FileStateEntry> fileState = new HashMap<>();
    /** docId → fileName (null for orphan/stale deletes from startup-sync) */
    private final Map<String, String> pendingDeletes = new HashMap<>();
    private final Map<String, PendingUpload> pendingUploads = new HashMap<>();

    public record FileStateEntry(String hash, long lastModified, String docId) {}

    public record PendingUpload(String fileName, String hash, Instant uploadedAt) {}

    private record PersistedState(
            Map<String, FileStateEntry> files,
            Map<String, String> pendingDeletes) {}

    public FileStateStore(@Value("${datensenke.state-file-path:data/datensenke-state.json}") String stateFilePath) {
        this.stateFile = Path.of(stateFilePath);
    }

    /**
     * Loads the persisted state from disk.
     * Populates pendingDeletes directly into the store.
     * Returns the file entries as a snapshot (does NOT modify live fileState).
     */
    public Map<String, FileStateEntry> loadSnapshot() {
        if (!Files.exists(stateFile)) {
            log.info("No persisted state file found at {}", stateFile);
            return Map.of();
        }
        try {
            JsonNode root = objectMapper.readTree(stateFile.toFile());

            if (root.has("files")) {
                // New format: { "files": {...}, "pendingDeletes": {...} }
                Map<String, FileStateEntry> files = objectMapper.convertValue(
                        root.get("files"), new TypeReference<Map<String, FileStateEntry>>() {});
                if (root.has("pendingDeletes")) {
                    Map<String, String> pd = objectMapper.convertValue(
                            root.get("pendingDeletes"), new TypeReference<Map<String, String>>() {});
                    pendingDeletes.putAll(pd);
                    log.info("Loaded {} pending delete(s) from state file", pd.size());
                }
                log.info("Loaded persisted state: {} file entries from {}", files.size(), stateFile);
                return files;
            } else {
                // Old format: flat map of file entries
                Map<String, FileStateEntry> files = objectMapper.convertValue(
                        root, new TypeReference<Map<String, FileStateEntry>>() {});
                log.info("Loaded persisted state (legacy format): {} entries from {}", files.size(), stateFile);
                return files;
            }
        } catch (Exception e) {
            log.warn("Failed to load state file: {}", e.getMessage());
            return Map.of();
        }
    }

    public void save() {
        try {
            Path parent = stateFile.getParent();
            if (parent != null) Files.createDirectories(parent);
            PersistedState state = new PersistedState(fileState, pendingDeletes);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(stateFile.toFile(), state);
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

    /** @param fileName the source file name, or null for orphan/stale startup-sync deletes */
    public void addPendingDelete(String docId, String fileName) { pendingDeletes.put(docId, fileName); }

    public void removePendingDelete(String docId) { pendingDeletes.remove(docId); }

    public Map<String, String> getPendingDeletes() { return pendingDeletes; }

    // ── Pending Uploads ─────────────────────────────────────────────────

    public Map<String, PendingUpload> getPendingUploads() { return pendingUploads; }

    public void addPendingUpload(String trackId, PendingUpload upload) { pendingUploads.put(trackId, upload); }

    public void removePendingUpload(String trackId) { pendingUploads.remove(trackId); }
}
