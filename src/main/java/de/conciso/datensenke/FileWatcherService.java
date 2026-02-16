package de.conciso.datensenke;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
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
    @Deprecated // Migration support: only needed for legacy docs with datensenke- prefix
    private static final String HASH_PREFIX = "datensenke-";
    @Deprecated // Migration support: only needed for legacy docs with datensenke- prefix
    private static final int MD5_HEX_LENGTH = 32;
    private static final Path STATE_FILE = Path.of(".datensenke-state.json");

    private final RemoteFileSource remoteFileSource;
    private final LightRagClient lightRagClient;
    private final String startupSync;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // fileName → {hash, lastModified, docId} — persisted to .datensenke-state.json
    private final Map<String, FileStateEntry> fileState = new HashMap<>();
    // Doc-IDs whose delete was rejected by LightRAG (status "busy").
    private final Set<String> pendingDeleteIds = new HashSet<>();

    public record FileStateEntry(String hash, long lastModified, String docId) {}

    record UploadResult(String hash, String docId) {}

    record PendingUpload(String fileName, String hash, Instant uploadedAt) {}

    private final FailureLogWriter failureLogWriter;
    // trackId → PendingUpload — uploads awaiting async processing confirmation
    private final Map<String, PendingUpload> pendingUploads = new HashMap<>();

    public FileWatcherService(
            RemoteFileSource remoteFileSource,
            LightRagClient lightRagClient,
            FailureLogWriter failureLogWriter,
            @Value("${datensenke.startup-sync:none}") String startupSync) {
        this.remoteFileSource = remoteFileSource;
        this.lightRagClient = lightRagClient;
        this.failureLogWriter = failureLogWriter;
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

        logUnreportedFailures();

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
                // Preserve docId from persisted state if available (migration)
                String docId = persisted != null ? persisted.docId() : null;
                fileState.put(fileName, new FileStateEntry(null, lastModified, docId));
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
                    UploadResult result = downloadAndUpload(sourceName);
                    fileState.put(sourceName, new FileStateEntry(result.hash(), state.lastModified(), result.docId()));
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

                        // Find newest doc and determine if stale
                        LightRagClient.DocumentInfo newest = matches.stream()
                                .max(Comparator.comparing(d -> d.created_at() != null ? d.created_at() : ""))
                                .orElseThrow();

                        boolean hashMatch;
                        if (state.docId() != null) {
                            // New mode: docId in state → compare via persisted hash
                            hashMatch = localHash.equals(state.hash());
                            log.debug("Startup-Sync: comparing via persisted state hash for {}", sourceName);
                        } else {
                            // Migration fallback: extract hash from filename in LightRAG
                            String embeddedHash = extractHash(newest.file_path());
                            hashMatch = localHash.equals(embeddedHash);
                            log.debug("Startup-Sync: comparing via embedded hash for {} (migration)", sourceName);
                        }

                        if (hashMatch) {
                            log.debug("Startup-Sync OK: {} (hash match)", sourceName);
                            // Store/update docId from LightRAG match
                            String docId = state.docId() != null ? state.docId() : newest.id();
                            // In full mode, clean up duplicates
                            if ("full".equalsIgnoreCase(startupSync)) {
                                for (LightRagClient.DocumentInfo dup : matches) {
                                    if (!dup.id().equals(newest.id())) {
                                        deleted += syncDelete(dup, "duplicate");
                                    }
                                }
                            }
                            fileState.put(sourceName, new FileStateEntry(localHash, state.lastModified(), docId));
                        } else {
                            // Content changed or legacy upload (no embedded hash)
                            log.info("Startup-Sync STALE: {}", sourceName);
                            stale++;
                            for (LightRagClient.DocumentInfo doc : matches) {
                                deleted += syncDelete(doc, "stale");
                            }
                            // Re-upload with original filename
                            if (downloadedFile == null) {
                                downloadedFile = remoteFileSource.downloadFile(sourceName);
                            }
                            Path uploadFile = downloadedFile.resolveSibling(sourceName);
                            Files.move(downloadedFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
                            String trackId = lightRagClient.uploadDocument(uploadFile);
                            Files.deleteIfExists(uploadFile);
                            downloadedFile = null;
                            String docId = resolveDocId(trackId, sourceName);
                            fileState.put(sourceName, new FileStateEntry(localHash, state.lastModified(), docId));
                            uploaded++;
                        }
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

    private void logUnreportedFailures() {
        try {
            List<LightRagClient.DocumentInfo> failedDocs =
                    lightRagClient.getDocumentsByStatus().getOrDefault("failed", List.of());
            if (failedDocs.isEmpty()) {
                return;
            }
            int logged = 0;
            for (LightRagClient.DocumentInfo doc : failedDocs) {
                if (!failureLogWriter.isAlreadyLogged(doc.track_id(), doc.created_at())) {
                    String reason = doc.error_msg() != null ? doc.error_msg() : "LightRAG status: failed";
                    failureLogWriter.logFailure(doc.file_path(), reason, doc.track_id(), null, doc.created_at());
                    logged++;
                }
            }
            if (logged > 0) {
                log.info("Startup: logged {} previously unreported failure(s)", logged);
            }
        } catch (Exception e) {
            log.warn("Startup: failed to check for unreported failures: {}", e.getMessage());
        }
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
        checkPendingUploads();

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

    private void checkPendingUploads() {
        if (pendingUploads.isEmpty()) {
            return;
        }
        log.info("Checking {} pending upload(s)", pendingUploads.size());
        Map<String, List<LightRagClient.DocumentInfo>> docsByStatus;
        try {
            docsByStatus = lightRagClient.getDocumentsByStatus();
        } catch (Exception e) {
            log.warn("Failed to fetch document statuses for pending upload check: {}", e.getMessage());
            return;
        }

        var iterator = pendingUploads.entrySet().iterator();
        boolean changed = false;
        while (iterator.hasNext()) {
            var entry = iterator.next();
            String trackId = entry.getKey();
            PendingUpload pending = entry.getValue();

            // Search for this trackId across all status groups
            String foundStatus = null;
            LightRagClient.DocumentInfo foundDoc = null;
            for (var statusEntry : docsByStatus.entrySet()) {
                for (LightRagClient.DocumentInfo doc : statusEntry.getValue()) {
                    if (trackId.equals(doc.track_id())) {
                        foundStatus = statusEntry.getKey();
                        foundDoc = doc;
                        break;
                    }
                }
                if (foundDoc != null) break;
            }

            if (foundDoc != null && "processed".equalsIgnoreCase(foundStatus)) {
                log.info("Pending upload processed: {} (docId={})", pending.fileName(), foundDoc.id());
                FileStateEntry state = fileState.get(pending.fileName());
                if (state != null) {
                    fileState.put(pending.fileName(),
                            new FileStateEntry(state.hash(), state.lastModified(), foundDoc.id()));
                    changed = true;
                }
                iterator.remove();
            } else if (foundDoc != null && "failed".equalsIgnoreCase(foundStatus)) {
                String reason = foundDoc.error_msg() != null ? foundDoc.error_msg() : "LightRAG status: failed";
                log.error("Upload failed in LightRAG: {} (trackId={}, reason={})", pending.fileName(), trackId, reason);
                failureLogWriter.logFailure(pending.fileName(), reason, trackId, pending.hash(), foundDoc.created_at());
                iterator.remove();
            } else if (foundStatus == null) {
                // Not found at all — might have disappeared; log as failure
                log.warn("Pending upload not found in LightRAG: {} (trackId={})", pending.fileName(), trackId);
                failureLogWriter.logFailure(pending.fileName(), "Document not found in LightRAG after upload",
                        trackId, pending.hash(), null);
                iterator.remove();
            }
            // else: still processing — leave in pendingUploads for next cycle
        }
        if (changed) {
            saveState();
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
                    UploadResult result = downloadAndUpload(fileName);
                    fileState.put(fileName, new FileStateEntry(result.hash(), lastModified, result.docId()));
                    changed = true;
                } else if (state.lastModified() != lastModified) {
                    log.info("UPDATE: {} (delete + re-upload)", fileName);
                    deleteByDocId(fileName);
                    UploadResult result = downloadAndUpload(fileName);
                    fileState.put(fileName, new FileStateEntry(result.hash(), lastModified, result.docId()));
                    changed = true;
                }
            } catch (LightRagBusyException e) {
                // LightRAG is still processing — keep old lastModified in fileState
                // so the next poll retries the update. Doc-ID is already in pendingDeleteIds.
                log.warn("UPDATE deferred (LightRAG busy): {}", fileName);
            } catch (Exception e) {
                log.error("Failed to process {}: {}", fileName, e.getMessage());
                FileStateEntry state = fileState.get(fileName);
                failureLogWriter.logFailure(fileName, e.getMessage(),
                        null, state != null ? state.hash() : null, null);
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
                deleteByDocId(fileName);
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

    private UploadResult downloadAndUpload(String fileName) {
        Path tempFile = remoteFileSource.downloadFile(fileName);
        try {
            String hash = computeFileHash(tempFile);
            // Upload with original filename (no prefix renaming)
            Path uploadFile = tempFile.resolveSibling(fileName);
            Files.move(tempFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
            try {
                String trackId = lightRagClient.uploadDocument(uploadFile);
                // Track pending upload for async status verification
                if (trackId != null) {
                    pendingUploads.put(trackId, new PendingUpload(fileName, hash, Instant.now()));
                }
                String docId = resolveDocId(trackId, fileName);
                if (docId != null && trackId != null) {
                    // Already resolved — no need to keep in pending
                    pendingUploads.remove(trackId);
                }
                return new UploadResult(hash, docId);
            } finally {
                Files.deleteIfExists(uploadFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare upload for " + fileName, e);
        } finally {
            try { Files.deleteIfExists(tempFile); } catch (Exception ignored) {}
        }
    }

    /**
     * Deletes a document from LightRAG.
     * Primary: uses stored docId from state.
     * Fallback (migration): searches by file_path if docId is null.
     */
    private void deleteByDocId(String fileName) {
        FileStateEntry state = fileState.get(fileName);
        String docId = state != null ? state.docId() : null;

        if (docId != null) {
            try {
                lightRagClient.deleteDocument(docId);
            } catch (LightRagBusyException e) {
                pendingDeleteIds.add(docId);
                throw e;
            }
            return;
        }

        // Fallback: search by file_path (migration from old state without docId)
        log.debug("No docId in state for {}, falling back to file_path search", fileName);
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

    /**
     * Resolves the LightRAG doc_id after an upload by querying GET /documents
     * and matching on track_id first, then falling back to file_path.
     */
    private String resolveDocId(String trackId, String fileName) {
        try {
            var docsByStatus = lightRagClient.getDocumentsByStatus();

            // Check for immediate failure
            if (trackId != null) {
                List<LightRagClient.DocumentInfo> failedDocs = docsByStatus.getOrDefault("failed", List.of());
                for (LightRagClient.DocumentInfo doc : failedDocs) {
                    if (trackId.equals(doc.track_id())) {
                        String reason = doc.error_msg() != null ? doc.error_msg() : "LightRAG status: failed";
                        log.error("Upload immediately failed in LightRAG: {} (trackId={}, reason={})", fileName, trackId, reason);
                        FileStateEntry state = fileState.get(fileName);
                        failureLogWriter.logFailure(fileName, reason,
                                trackId, state != null ? state.hash() : null, doc.created_at());
                        pendingUploads.remove(trackId);
                        return null;
                    }
                }
            }

            // Flatten all docs for resolution
            List<LightRagClient.DocumentInfo> allDocs = new ArrayList<>();
            docsByStatus.values().forEach(allDocs::addAll);

            // Primary: match by track_id
            if (trackId != null) {
                var match = allDocs.stream()
                        .filter(doc -> trackId.equals(doc.track_id()))
                        .findFirst();
                if (match.isPresent()) {
                    log.debug("Resolved docId by track_id for {}: {}", fileName, match.get().id());
                    return match.get().id();
                }
            }
            // Fallback: match by file_path
            var match = allDocs.stream()
                    .filter(doc -> doc.file_path() != null && doc.file_path().endsWith(fileName))
                    .findFirst();
            if (match.isPresent()) {
                log.debug("Resolved docId by file_path for {}: {}", fileName, match.get().id());
                return match.get().id();
            }
        } catch (Exception e) {
            log.warn("Failed to resolve docId for {}: {}", fileName, e.getMessage());
        }
        log.debug("Could not resolve docId for {}", fileName);
        return null;
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
     * @deprecated Migration support only — new uploads use docId mapping instead.
     */
    @Deprecated
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
