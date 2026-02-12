package de.conciso.datensenke;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FileWatcherService {

    private static final Logger log = LoggerFactory.getLogger(FileWatcherService.class);
    private static final String HASH_PREFIX = "datensenke-";
    private static final int MD5_HEX_LENGTH = 32;
    private static final Path STATE_FILE = Path.of(".datensenke-state.json");

    private final RemoteFileSource remoteFileSource;
    private final LightRagClient lightRagClient;
    private final String startupSync;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // fileName → {hash, lastModified} — persisted to .datensenke-state.json
    private final Map<String, FileStateEntry> fileState = new HashMap<>();
    // Doc-IDs whose delete was rejected by LightRAG (status "busy").
    private final Set<String> pendingDeleteIds = new HashSet<>();

    public record FileStateEntry(String hash, long lastModified) {}

    public FileWatcherService(
            RemoteFileSource remoteFileSource,
            LightRagClient lightRagClient,
            @Value("${datensenke.startup-sync:none}") String startupSync) {
        this.remoteFileSource = remoteFileSource;
        this.lightRagClient = lightRagClient;
        this.startupSync = startupSync;
    }

    // ── State persistence ───────────────────────────────────────────────

    private Map<String, FileStateEntry> loadPersistedState() {
        if (!Files.exists(STATE_FILE)) {
            log.info("No persisted state file found");
            return Map.of();
        }
        try {
            Map<String, FileStateEntry> state = objectMapper.readValue(
                    STATE_FILE.toFile(),
                    new TypeReference<Map<String, FileStateEntry>>() {});
            log.info("Loaded persisted state: {} entries", state.size());
            return state;
        } catch (Exception e) {
            log.warn("Failed to load state file: {}", e.getMessage());
            return Map.of();
        }
    }

    private void saveState() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(STATE_FILE.toFile(), fileState);
        } catch (Exception e) {
            log.warn("Failed to save state file: {}", e.getMessage());
        }
    }

    // ── Startup sync ────────────────────────────────────────────────────

    @EventListener(ApplicationReadyEvent.class)
    public void syncOnStartup() {
        log.info("Startup-Sync mode: {}", startupSync);

        Map<String, FileStateEntry> persistedState = loadPersistedState();

        List<RemoteFileInfo> currentFiles = remoteFileSource.listPdfFiles();
        Map<String, Long> currentFileMap = currentFiles.stream()
                .collect(Collectors.toMap(RemoteFileInfo::fileName, RemoteFileInfo::lastModified));

        // Pre-populate fileState: reuse persisted hash if lastModified unchanged
        for (var entry : currentFileMap.entrySet()) {
            String fileName = entry.getKey();
            long lastModified = entry.getValue();
            FileStateEntry persisted = persistedState.get(fileName);

            if (persisted != null && persisted.lastModified() == lastModified && persisted.hash() != null) {
                fileState.put(fileName, persisted);
            } else {
                fileState.put(fileName, new FileStateEntry(null, lastModified));
            }
        }

        if ("none".equalsIgnoreCase(startupSync)) {
            log.info("Startup-Sync: none — fileState pre-populated with {} files, skipping LightRAG sync",
                    currentFileMap.size());
            saveState();
            return;
        }

        List<LightRagClient.DocumentInfo> lightragDocs = lightRagClient.getDocuments();
        List<LightRagClient.DocumentInfo> docsWithPath = lightragDocs.stream()
                .filter(doc -> doc.file_path() != null)
                .toList();

        Set<String> sourceFileNames = currentFileMap.keySet();

        // Group LightRAG docs by matching source file name
        Map<String, List<LightRagClient.DocumentInfo>> docsBySourceFile = new HashMap<>();
        for (LightRagClient.DocumentInfo doc : docsWithPath) {
            for (String sourceName : sourceFileNames) {
                if (doc.file_path().endsWith(sourceName)) {
                    docsBySourceFile.computeIfAbsent(sourceName, k -> new ArrayList<>()).add(doc);
                    break;
                }
            }
        }

        int uploaded = 0, deleted = 0, stale = 0;

        for (String sourceName : sourceFileNames) {
            List<LightRagClient.DocumentInfo> matches = docsBySourceFile.getOrDefault(sourceName, List.of());
            FileStateEntry state = fileState.get(sourceName);

            try {
                if (matches.isEmpty()) {
                    // Missing in LightRAG → upload
                    log.info("Startup-Sync UPLOAD (missing): {}", sourceName);
                    String hash = downloadAndUpload(sourceName);
                    fileState.put(sourceName, new FileStateEntry(hash, state.lastModified()));
                    uploaded++;
                } else {
                    // Has match in LightRAG — check content hash
                    String localHash = state.hash();
                    Path downloadedFile = null;

                    try {
                        if (localHash == null) {
                            // lastModified changed or no persisted state → download to compute hash
                            downloadedFile = remoteFileSource.downloadFile(sourceName);
                            localHash = computeFileHash(downloadedFile);
                            log.debug("Startup-Sync: computed hash for {} (file changed or no persisted state)",
                                    sourceName);
                        } else {
                            log.debug("Startup-Sync: using persisted hash for {} (lastModified unchanged)",
                                    sourceName);
                        }

                        // Find newest doc
                        LightRagClient.DocumentInfo newest = matches.stream()
                                .max(Comparator.comparing(d -> d.created_at() != null ? d.created_at() : ""))
                                .orElseThrow();
                        String embeddedHash = extractHash(newest.file_path());

                        if (localHash.equals(embeddedHash)) {
                            log.debug("Startup-Sync OK: {} (hash match)", sourceName);
                            // In full mode, clean up duplicates
                            if ("full".equalsIgnoreCase(startupSync)) {
                                for (LightRagClient.DocumentInfo dup : matches) {
                                    if (!dup.id().equals(newest.id())) {
                                        deleted += syncDelete(dup, "duplicate");
                                    }
                                }
                            }
                        } else {
                            // Content changed or legacy upload (no embedded hash)
                            log.info("Startup-Sync STALE: {} (local={}, lightrag={})",
                                    sourceName, localHash, embeddedHash != null ? embeddedHash : "none");
                            stale++;
                            for (LightRagClient.DocumentInfo doc : matches) {
                                deleted += syncDelete(doc, "stale");
                            }
                            // Re-upload — reuse already downloaded file if available
                            if (downloadedFile == null) {
                                downloadedFile = remoteFileSource.downloadFile(sourceName);
                            }
                            Path uploadFile = downloadedFile.resolveSibling(
                                    "datensenke-" + localHash + "-" + sourceName);
                            Files.move(downloadedFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
                            lightRagClient.uploadDocument(uploadFile);
                            Files.deleteIfExists(uploadFile);
                            downloadedFile = null;
                            uploaded++;
                        }

                        fileState.put(sourceName, new FileStateEntry(localHash, state.lastModified()));
                    } finally {
                        if (downloadedFile != null) {
                            try { Files.deleteIfExists(downloadedFile); } catch (Exception ignored) {}
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Startup-Sync failed for {}: {}", sourceName, e.getMessage());
            }
        }

        // In full mode, delete orphans (docs that match no source file)
        if ("full".equalsIgnoreCase(startupSync)) {
            List<LightRagClient.DocumentInfo> orphaned = docsWithPath.stream()
                    .filter(doc -> sourceFileNames.stream().noneMatch(doc.file_path()::endsWith))
                    .toList();
            for (LightRagClient.DocumentInfo doc : orphaned) {
                deleted += syncDelete(doc, "orphan");
            }
        }

        log.info("Startup-Sync completed: {} uploaded ({} stale re-uploads), {} deleted, {} deferred",
                uploaded, stale, deleted, pendingDeleteIds.size());
        saveState();
    }

    private int syncDelete(LightRagClient.DocumentInfo doc, String reason) {
        try {
            log.info("Startup-Sync DELETE {}: {} (id={})", reason, doc.file_path(), doc.id());
            lightRagClient.deleteDocument(doc.id());
            return 1;
        } catch (LightRagBusyException e) {
            log.warn("Startup-Sync DELETE deferred (busy): {} — will retry on next poll", doc.id());
            pendingDeleteIds.add(doc.id());
            return 0;
        } catch (Exception e) {
            log.error("Startup-Sync failed to delete {}: {}", doc.id(), e.getMessage());
            return 0;
        }
    }

    // ── Polling ─────────────────────────────────────────────────────────

    @Scheduled(fixedDelayString = "${datensenke.poll-interval-ms}")
    public void poll() {
        log.debug("Polling remote directory");

        retryPendingDeletes();

        List<RemoteFileInfo> currentFiles = remoteFileSource.listPdfFiles();
        Map<String, Long> currentFileMap = currentFiles.stream()
                .collect(Collectors.toMap(RemoteFileInfo::fileName, RemoteFileInfo::lastModified));

        boolean changed = handleNewAndUpdatedFiles(currentFileMap);
        changed |= handleDeletedFiles(currentFileMap.keySet());

        if (changed) {
            saveState();
        }
    }

    private void retryPendingDeletes() {
        if (pendingDeleteIds.isEmpty()) {
            return;
        }
        log.info("Retrying {} pending delete(s)", pendingDeleteIds.size());
        var iterator = pendingDeleteIds.iterator();
        while (iterator.hasNext()) {
            String docId = iterator.next();
            try {
                lightRagClient.deleteDocument(docId);
                iterator.remove();
                log.info("Retry-delete successful: {}", docId);
            } catch (LightRagBusyException e) {
                log.warn("Retry-delete still busy: {}", docId);
            } catch (Exception e) {
                log.error("Retry-delete failed for {}: {}", docId, e.getMessage());
                iterator.remove();
            }
        }
    }

    private boolean handleNewAndUpdatedFiles(Map<String, Long> currentFiles) {
        boolean changed = false;
        for (var entry : currentFiles.entrySet()) {
            String fileName = entry.getKey();
            long lastModified = entry.getValue();

            try {
                FileStateEntry state = fileState.get(fileName);
                if (state == null) {
                    log.info("CREATE: {}", fileName);
                    String hash = downloadAndUpload(fileName);
                    fileState.put(fileName, new FileStateEntry(hash, lastModified));
                    changed = true;
                } else if (state.lastModified() != lastModified) {
                    log.info("UPDATE: {} (delete + re-upload)", fileName);
                    deleteByFileName(fileName);
                    String hash = downloadAndUpload(fileName);
                    fileState.put(fileName, new FileStateEntry(hash, lastModified));
                    changed = true;
                }
            } catch (LightRagBusyException e) {
                // LightRAG is still processing — keep old lastModified in fileState
                // so the next poll retries the update. Doc-ID is already in pendingDeleteIds.
                log.warn("UPDATE deferred (LightRAG busy): {}", fileName);
            } catch (Exception e) {
                log.error("Failed to process {}: {}", fileName, e.getMessage());
            }
        }
        return changed;
    }

    private boolean handleDeletedFiles(Set<String> currentFileNames) {
        boolean changed = false;
        var removedFiles = fileState.keySet().stream()
                .filter(name -> !currentFileNames.contains(name))
                .toList();

        for (String fileName : removedFiles) {
            try {
                log.info("DELETE: {}", fileName);
                deleteByFileName(fileName);
                fileState.remove(fileName);
                changed = true;
            } catch (LightRagBusyException e) {
                log.warn("DELETE deferred (LightRAG busy): {}", fileName);
            } catch (Exception e) {
                log.error("Failed to delete {}: {}", fileName, e.getMessage());
                fileState.remove(fileName);
                changed = true;
            }
        }
        return changed;
    }

    // ── Upload / Delete helpers ─────────────────────────────────────────

    private String downloadAndUpload(String fileName) {
        Path tempFile = remoteFileSource.downloadFile(fileName);
        Path uploadFile = tempFile;
        try {
            String hash = computeFileHash(tempFile);
            uploadFile = tempFile.resolveSibling("datensenke-" + hash + "-" + fileName);
            Files.move(tempFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
            lightRagClient.uploadDocument(uploadFile);
            return hash;
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare upload for " + fileName, e);
        } finally {
            try {
                Files.deleteIfExists(tempFile);
                if (!uploadFile.equals(tempFile)) {
                    Files.deleteIfExists(uploadFile);
                }
            } catch (Exception e) {
                log.warn("Failed to clean up temp files for {}: {}", fileName, e.getMessage());
            }
        }
    }

    private void deleteByFileName(String fileName) {
        var documents = lightRagClient.getDocuments();
        documents.stream()
                .filter(doc -> doc.file_path() != null && doc.file_path().endsWith(fileName))
                .findFirst()
                .ifPresentOrElse(
                        doc -> {
                            try {
                                lightRagClient.deleteDocument(doc.id());
                            } catch (LightRagBusyException e) {
                                pendingDeleteIds.add(doc.id());
                                throw e;
                            }
                        },
                        () -> log.warn("Document not found in LightRAG for file: {}", fileName)
                );
    }

    // ── Hash utilities ──────────────────────────────────────────────────

    static String computeFileHash(Path file) {
        try (InputStream is = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute hash for " + file, e);
        }
    }

    /**
     * Extracts the embedded MD5 hash from a LightRAG file_path.
     * Expected pattern: datensenke-{32 hex chars}-{originalName}
     * Returns null if the file_path doesn't contain a valid embedded hash (legacy uploads).
     */
    static String extractHash(String filePath) {
        String name = filePath.substring(filePath.lastIndexOf('/') + 1);
        if (!name.startsWith(HASH_PREFIX)) {
            return null;
        }
        String afterPrefix = name.substring(HASH_PREFIX.length());
        if (afterPrefix.length() <= MD5_HEX_LENGTH || afterPrefix.charAt(MD5_HEX_LENGTH) != '-') {
            return null;
        }
        String hash = afterPrefix.substring(0, MD5_HEX_LENGTH);
        return hash.matches("[0-9a-f]{32}") ? hash : null;
    }
}
