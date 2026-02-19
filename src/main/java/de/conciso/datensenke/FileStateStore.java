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

/**
 * Persists the synchronisation state between remote file source and LightRAG across restarts.
 *
 * <p>Three maps are maintained and written to a single JSON state file:
 * <ul>
 *   <li>{@code fileState} — one entry per known source file, tracking its MD5 hash,
 *       last-modified timestamp, and the LightRAG doc-id it was uploaded as.</li>
 *   <li>{@code pendingDeletes} — LightRAG doc-ids whose deletion failed with "busy"
 *       and must be retried. Each entry also carries the source file name and a flag
 *       indicating whether the file should be re-uploaded once the delete succeeds
 *       (used when a stale-file update was interrupted mid-way).</li>
 *   <li>{@code pendingUploads} — uploads whose processing status in LightRAG is still
 *       unknown (async processing); resolved on the next poll cycle.</li>
 * </ul>
 *
 * <p>State file format (JSON):
 * <pre>
 * {
 *   "files":          { "&lt;fileName&gt;": { "hash": "...", "lastModified": 0, "docId": "..." }, ... },
 *   "pendingDeletes": { "&lt;docId&gt;":   { "fileName": "...", "reuploadOnSuccess": false }, ... }
 * }
 * </pre>
 * The legacy format (flat map of file entries without the wrapper) is loaded transparently.
 */
@Component
public class FileStateStore {

    private static final Logger log = LoggerFactory.getLogger(FileStateStore.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path stateFile;

    private final Map<String, FileStateEntry> fileState = new HashMap<>();
    /** docId → PendingDelete */
    private final Map<String, PendingDelete> pendingDeletes = new HashMap<>();
    private final Map<String, PendingUpload> pendingUploads = new HashMap<>();

    public record FileStateEntry(String hash, long lastModified, String docId) {}

    public record PendingUpload(String fileName, String hash, Instant uploadedAt) {}

    /**
     * Represents a LightRAG document deletion that could not be completed immediately
     * because LightRAG was busy, and must be retried.
     *
     * @param fileName          source file name associated with this doc-id, or {@code null}
     *                          for orphan/duplicate deletes triggered during startup-sync
     *                          (where no further action is needed after the delete)
     * @param reuploadOnSuccess {@code true} if the source file should be re-uploaded to LightRAG
     *                          once this delete succeeds. Set when a stale-file update (delete +
     *                          re-upload) was interrupted: the delete was deferred because LightRAG
     *                          was busy, and uploading the new version immediately would fail with
     *                          "document already exists". The re-upload is deferred until the delete
     *                          goes through.
     */
    public record PendingDelete(String fileName, boolean reuploadOnSuccess) {}

    private record PersistedState(
            Map<String, FileStateEntry> files,
            Map<String, PendingDelete> pendingDeletes) {}

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
                    Map<String, PendingDelete> pd = objectMapper.convertValue(
                            root.get("pendingDeletes"), new TypeReference<Map<String, PendingDelete>>() {});
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

    public void addPendingDelete(String docId, PendingDelete entry) { pendingDeletes.put(docId, entry); }

    public void removePendingDelete(String docId) { pendingDeletes.remove(docId); }

    public Map<String, PendingDelete> getPendingDeletes() { return pendingDeletes; }

    // ── Pending Uploads ─────────────────────────────────────────────────

    public Map<String, PendingUpload> getPendingUploads() { return pendingUploads; }

    public void addPendingUpload(String trackId, PendingUpload upload) { pendingUploads.put(trackId, upload); }

    public void removePendingUpload(String trackId) { pendingUploads.remove(trackId); }
}
