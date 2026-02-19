package de.conciso.datensenke;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import de.conciso.datensenke.preprocessor.FilePreprocessor;
import de.conciso.datensenke.remote.RemoteFileInfo;
import de.conciso.datensenke.remote.RemoteFileSource;
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

    private final RemoteFileSource remoteFileSource;
    private final LightRagClient lightRagClient;
    private final FailureLogWriter failureLogWriter;
    private final FileStateStore store;
    private final FilePreprocessor preprocessor;
    private final String startupSync;
    private final boolean cleanupFailedDocs;

    record UploadResult(String hash, String docId) {}

    private record SyncStats(int uploaded, int deleted, int stale) {
        static SyncStats zero() { return new SyncStats(0, 0, 0); }
        SyncStats add(SyncStats other) {
            return new SyncStats(uploaded + other.uploaded, deleted + other.deleted, stale + other.stale);
        }
    }

    public FileWatcherService(
            RemoteFileSource remoteFileSource,
            LightRagClient lightRagClient,
            FailureLogWriter failureLogWriter,
            FileStateStore store,
            FilePreprocessor preprocessor,
            @Value("${datensenke.startup-sync:none}") String startupSync,
            @Value("${datensenke.cleanup-failed-docs:false}") boolean cleanupFailedDocs) {
        this.remoteFileSource = remoteFileSource;
        this.lightRagClient = lightRagClient;
        this.failureLogWriter = failureLogWriter;
        this.store = store;
        this.preprocessor = preprocessor;
        this.startupSync = startupSync;
        this.cleanupFailedDocs = cleanupFailedDocs;
    }

    // ── Startup sync ────────────────────────────────────────────────────

    @EventListener(ApplicationReadyEvent.class)
    public void syncOnStartup() {
        log.info("Startup-Sync mode: {}", startupSync);

        logUnreportedFailures();

        Map<String, Long> currentFileMap = listRemoteFiles();
        Map<String, FileStateStore.FileStateEntry> persistedState = store.loadSnapshot();

        retryPendingDeletesOnStartup(currentFileMap.keySet());

        prepopulateFileState(currentFileMap, persistedState);

        if ("none".equalsIgnoreCase(startupSync)) {
            log.info("Startup-Sync: none — {} files pre-populated, skipping LightRAG sync", currentFileMap.size());
            store.save();
            return;
        }

        List<LightRagClient.DocumentInfo> docsWithPath = fetchDocsWithPath();
        Map<String, List<LightRagClient.DocumentInfo>> docsBySourceFile =
                groupDocsBySourceFile(docsWithPath, currentFileMap.keySet());

        SyncStats stats = syncAllFiles(currentFileMap.keySet(), docsBySourceFile);

        if ("full".equalsIgnoreCase(startupSync)) {
            int orphansDeleted = deleteOrphans(docsWithPath, currentFileMap.keySet());
            stats = new SyncStats(stats.uploaded(), stats.deleted() + orphansDeleted, stats.stale());
        }

        log.info("Startup-Sync completed: {} uploaded ({} stale re-uploads), {} deleted, {} deferred",
                stats.uploaded(), stats.stale(), stats.deleted(), store.getPendingDeletes().size());
        store.save();
    }

    private Map<String, Long> listRemoteFiles() {
        return remoteFileSource.listFiles().stream()
                .collect(Collectors.toMap(RemoteFileInfo::fileName, RemoteFileInfo::lastModified));
    }

    private void prepopulateFileState(Map<String, Long> currentFileMap,
                                      Map<String, FileStateStore.FileStateEntry> persistedState) {
        for (var entry : currentFileMap.entrySet()) {
            String fileName = entry.getKey();
            long lastModified = entry.getValue();
            FileStateStore.FileStateEntry persisted = persistedState.get(fileName);

            if (persisted != null && persisted.lastModified() == lastModified && persisted.hash() != null) {
                store.putEntry(fileName, persisted);
            } else {
                String docId = persisted != null ? persisted.docId() : null;
                store.putEntry(fileName, new FileStateStore.FileStateEntry(null, lastModified, docId));
            }
        }
    }

    private List<LightRagClient.DocumentInfo> fetchDocsWithPath() {
        return lightRagClient.getDocuments().stream()
                .filter(d -> d.file_path() != null)
                .toList();
    }

    private Map<String, List<LightRagClient.DocumentInfo>> groupDocsBySourceFile(
            List<LightRagClient.DocumentInfo> docs, Set<String> sourceFileNames) {
        Map<String, List<LightRagClient.DocumentInfo>> result = new HashMap<>();
        for (LightRagClient.DocumentInfo doc : docs) {
            for (String sourceName : sourceFileNames) {
                if (doc.file_path().endsWith(sourceName)) {
                    result.computeIfAbsent(sourceName, k -> new ArrayList<>()).add(doc);
                    break;
                }
            }
        }
        return result;
    }

    private SyncStats syncAllFiles(Set<String> sourceFileNames,
                                   Map<String, List<LightRagClient.DocumentInfo>> docsBySourceFile) {
        SyncStats total = SyncStats.zero();
        for (String sourceName : sourceFileNames) {
            try {
                total = total.add(syncOneFile(sourceName, docsBySourceFile.getOrDefault(sourceName, List.of())));
            } catch (Exception e) {
                log.error("Startup-Sync failed for {}: {}", sourceName, e.getMessage());
            }
        }
        return total;
    }

    private SyncStats syncOneFile(String sourceName, List<LightRagClient.DocumentInfo> matches) throws Exception {
        FileStateStore.FileStateEntry state = store.getEntry(sourceName);
        if (matches.isEmpty()) {
            log.info("Startup-Sync UPLOAD (missing): {}", sourceName);
            UploadResult result = downloadAndUpload(sourceName);
            store.putEntry(sourceName, new FileStateStore.FileStateEntry(result.hash(), state.lastModified(), result.docId()));
            return new SyncStats(1, 0, 0);
        }
        return syncExistingFile(sourceName, matches, state);
    }

    private SyncStats syncExistingFile(String sourceName, List<LightRagClient.DocumentInfo> matches,
                                       FileStateStore.FileStateEntry state) throws Exception {
        String localHash = state.hash();
        Path downloadedFile = null;
        try {
            if (localHash == null) {
                downloadedFile = remoteFileSource.downloadFile(sourceName);
                localHash = computeFileHash(downloadedFile);
                log.debug("Startup-Sync: computed hash for {} (file changed or no persisted state)", sourceName);
            } else {
                log.debug("Startup-Sync: using persisted hash for {} (lastModified unchanged)", sourceName);
            }

            LightRagClient.DocumentInfo newest = findNewest(matches);
            boolean hashMatch = isHashMatch(localHash, state, sourceName);

            if (hashMatch) {
                return handleHashMatch(sourceName, state, matches, newest, localHash);
            } else {
                SyncStats result = handleHashMismatch(sourceName, state, matches, localHash, downloadedFile);
                downloadedFile = null; // ownership transferred — file was moved inside handleHashMismatch
                return result;
            }
        } finally {
            if (downloadedFile != null) {
                try { Files.deleteIfExists(downloadedFile); } catch (Exception ignored) {}
            }
        }
    }

    private LightRagClient.DocumentInfo findNewest(List<LightRagClient.DocumentInfo> docs) {
        return docs.stream()
                .max(Comparator.comparing(d -> d.created_at() != null ? d.created_at() : ""))
                .orElseThrow();
    }

    private boolean isHashMatch(String localHash, FileStateStore.FileStateEntry state, String sourceName) {
        if (state.docId() == null) {
            log.debug("Startup-Sync: no docId in state for {} — treating as stale", sourceName);
            return false;
        }
        return localHash.equals(state.hash());
    }

    private SyncStats handleHashMatch(String sourceName, FileStateStore.FileStateEntry state,
                                      List<LightRagClient.DocumentInfo> matches,
                                      LightRagClient.DocumentInfo newest, String localHash) {
        log.debug("Startup-Sync OK: {} (hash match)", sourceName);
        String docId = state.docId();
        int deleted = 0;
        if ("full".equalsIgnoreCase(startupSync)) {
            for (LightRagClient.DocumentInfo dup : matches) {
                if (!dup.id().equals(newest.id())) {
                    deleted += syncDelete(dup, "duplicate");
                }
            }
        }
        store.putEntry(sourceName, new FileStateStore.FileStateEntry(localHash, state.lastModified(), docId));
        return new SyncStats(0, deleted, 0);
    }

    private SyncStats handleHashMismatch(String sourceName, FileStateStore.FileStateEntry state,
                                         List<LightRagClient.DocumentInfo> matches,
                                         String localHash, Path alreadyDownloaded) throws Exception {
        log.info("Startup-Sync STALE: {}", sourceName);
        int deleted = 0;
        for (LightRagClient.DocumentInfo doc : matches) {
            deleted += syncDelete(doc, "stale");
        }

        // If any delete was deferred (LightRAG busy), uploading immediately would fail because
        // the old document is still present in LightRAG. Mark the pending delete with
        // reuploadOnSuccess=true so the upload is triggered once the delete goes through.
        boolean anyDeleteDeferred = matches.stream()
                .anyMatch(doc -> store.getPendingDeletes().containsKey(doc.id()));
        if (anyDeleteDeferred) {
            log.info("Startup-Sync STALE {}: delete deferred — upload will follow once delete succeeds", sourceName);
            matches.stream()
                    .filter(doc -> store.getPendingDeletes().containsKey(doc.id()))
                    .forEach(doc -> store.addPendingDelete(doc.id(),
                            new FileStateStore.PendingDelete(sourceName, true)));
            if (alreadyDownloaded == null) {
                // nothing to clean up — file was not downloaded by this path
            } else {
                try { Files.deleteIfExists(alreadyDownloaded); } catch (Exception ignored) {}
            }
            return new SyncStats(0, deleted, 0);
        }

        boolean ownedByUs = (alreadyDownloaded == null);
        Path tempFile = ownedByUs ? remoteFileSource.downloadFile(sourceName) : alreadyDownloaded;
        try {
            Path uploadFile = tempFile.resolveSibling(sourceName);
            Files.move(tempFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile = null; // moved — original path is gone
            String trackId = lightRagClient.uploadDocument(uploadFile);
            Files.deleteIfExists(uploadFile);
            String docId = resolveDocId(trackId, sourceName);
            store.putEntry(sourceName, new FileStateStore.FileStateEntry(localHash, state.lastModified(), docId));
        } finally {
            if (ownedByUs && tempFile != null) {
                try { Files.deleteIfExists(tempFile); } catch (Exception ignored) {}
            }
        }
        return new SyncStats(1, deleted, 1);
    }

    private int deleteOrphans(List<LightRagClient.DocumentInfo> docsWithPath, Set<String> sourceFileNames) {
        int deleted = 0;
        for (LightRagClient.DocumentInfo doc : docsWithPath) {
            if (sourceFileNames.stream().noneMatch(doc.file_path()::endsWith)) {
                deleted += syncDelete(doc, "orphan");
            }
        }
        return deleted;
    }

    private void logUnreportedFailures() {
        try {
            List<LightRagClient.DocumentInfo> failedDocs =
                    lightRagClient.getDocumentsByStatus().getOrDefault("failed", List.of());
            if (failedDocs.isEmpty()) return;
            int logged = 0, cleaned = 0;
            for (LightRagClient.DocumentInfo doc : failedDocs) {
                if (!failureLogWriter.isAlreadyLogged(doc.track_id(), doc.created_at())) {
                    String reason = doc.error_msg() != null ? doc.error_msg() : "LightRAG status: failed";
                    failureLogWriter.logFailure(doc.file_path(), reason, doc.track_id(), null, doc.created_at());
                    logged++;
                }
                if (cleanupFailedDocs) {
                    try {
                        lightRagClient.deleteDocument(doc.id());
                        cleaned++;
                    } catch (Exception e) {
                        log.warn("Failed to cleanup failed doc {} from LightRAG: {}", doc.id(), e.getMessage());
                    }
                }
            }
            if (logged > 0) log.info("Startup: logged {} previously unreported failure(s)", logged);
            if (cleaned > 0) log.info("Startup: cleaned up {} failed doc(s) from LightRAG", cleaned);
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
            log.warn("Startup-Sync DELETE deferred (busy): {} — will retry on startup/next poll", doc.id());
            store.addPendingDelete(doc.id(), new FileStateStore.PendingDelete(null, false));
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

        boolean changed = retryPendingDeletes();
        checkPendingUploads();

        List<RemoteFileInfo> currentFiles = remoteFileSource.listFiles();
        Map<String, Long> currentFileMap = currentFiles.stream()
                .collect(Collectors.toMap(RemoteFileInfo::fileName, RemoteFileInfo::lastModified));

        changed |= handleNewAndUpdatedFiles(currentFileMap);
        changed |= handleDeletedFiles(currentFileMap.keySet());

        if (changed) {
            store.save();
        }
    }

    /**
     * Downloads and uploads a file after its previous LightRAG document was successfully deleted.
     *
     * <p>Called when a pending delete with {@code reuploadOnSuccess=true} completes — i.e. a stale
     * file update where the old-document delete had to be deferred because LightRAG was busy.
     * Uploading the new version before the delete goes through would fail with "document already
     * exists" (LightRAG deduplicates by file name), so the upload is intentionally postponed until
     * this point.
     */
    private void reuploadAfterDelete(String fileName) {
        try {
            log.info("Re-uploading {} after deferred delete succeeded", fileName);
            UploadResult result = downloadAndUpload(fileName);
            FileStateStore.FileStateEntry existing = store.getEntry(fileName);
            long lastModified = existing != null ? existing.lastModified() : 0;
            store.putEntry(fileName, new FileStateStore.FileStateEntry(result.hash(), lastModified, result.docId()));
        } catch (Exception e) {
            log.error("Re-upload after delete failed for {}: {}", fileName, e.getMessage());
        }
    }

    /**
     * Retries pending deletes from a previous run before startup-sync begins.
     * Runs regardless of startup-sync mode.
     * If a file has returned to the remote source, it is still deleted from LightRAG —
     * the normal sync will then re-upload it as a new file.
     */
    private void retryPendingDeletesOnStartup(Set<String> currentRemoteFiles) {
        Map<String, FileStateStore.PendingDelete> pending = new java.util.HashMap<>(store.getPendingDeletes());
        if (pending.isEmpty()) return;
        log.info("Startup: retrying {} pending delete(s) from previous run", pending.size());
        for (var entry : pending.entrySet()) {
            String docId = entry.getKey();
            FileStateStore.PendingDelete pd = entry.getValue();
            try {
                lightRagClient.deleteDocument(docId);
                store.removePendingDelete(docId);
                if (pd.fileName() != null) store.removeEntry(pd.fileName());
                if (pd.reuploadOnSuccess() && pd.fileName() != null && currentRemoteFiles.contains(pd.fileName())) {
                    log.info("Startup: pending delete succeeded for {} — re-uploading stale file", pd.fileName());
                    reuploadAfterDelete(pd.fileName());
                } else if (pd.fileName() != null && currentRemoteFiles.contains(pd.fileName())) {
                    log.info("Startup: pending delete succeeded for {} (file returned — will be uploaded by sync)", pd.fileName());
                } else {
                    log.info("Startup: pending delete succeeded: docId={}, file={}", docId, pd.fileName());
                }
            } catch (LightRagBusyException e) {
                log.warn("Startup: pending delete still busy: docId={}, file={} — will retry on next poll", docId, pd.fileName());
            } catch (Exception e) {
                log.warn("Startup: pending delete failed, giving up: docId={}, file={}, error={}", docId, pd.fileName(), e.getMessage());
                store.removePendingDelete(docId);
                if (pd.fileName() != null) store.removeEntry(pd.fileName());
            }
        }
    }

    /**
     * Retries LightRAG document deletions that previously failed with "busy".
     * Must run at the start of each poll cycle, before new/updated/deleted file detection,
     * so that a successfully retried delete immediately makes room for a re-upload in the
     * same cycle (via {@code reuploadOnSuccess=true}) or causes the file to be detected
     * as a new CREATE by {@link #handleNewAndUpdatedFiles}.
     */
    private boolean retryPendingDeletes() {
        if (store.getPendingDeletes().isEmpty()) return false;
        log.info("Retrying {} pending delete(s)", store.getPendingDeletes().size());
        boolean changed = false;
        for (var entry : new java.util.HashMap<>(store.getPendingDeletes()).entrySet()) {
            String docId = entry.getKey();
            FileStateStore.PendingDelete pending = entry.getValue();
            try {
                lightRagClient.deleteDocument(docId);
                store.removePendingDelete(docId);
                if (pending.fileName() != null) store.removeEntry(pending.fileName());
                log.info("Retry-delete successful: docId={}, file={}", docId, pending.fileName());
                if (pending.reuploadOnSuccess() && pending.fileName() != null) {
                    reuploadAfterDelete(pending.fileName());
                }
                changed = true;
            } catch (LightRagBusyException e) {
                log.warn("Retry-delete still busy: docId={}", docId);
            } catch (Exception e) {
                log.error("Retry-delete failed for docId={}: {}", docId, e.getMessage());
                store.removePendingDelete(docId);
                if (pending.fileName() != null) store.removeEntry(pending.fileName());
                changed = true;
            }
        }
        return changed;
    }

    private void checkPendingUploads() {
        if (store.getPendingUploads().isEmpty()) return;
        log.info("Checking {} pending upload(s)", store.getPendingUploads().size());

        Map<String, List<LightRagClient.DocumentInfo>> docsByStatus;
        try {
            docsByStatus = lightRagClient.getDocumentsByStatus();
        } catch (Exception e) {
            log.warn("Failed to fetch document statuses for pending upload check: {}", e.getMessage());
            return;
        }

        boolean changed = false;
        for (var entry : new ArrayList<>(store.getPendingUploads().entrySet())) {
            changed |= processPendingUpload(entry.getKey(), entry.getValue(), docsByStatus);
        }
        if (changed) store.save();
    }

    private boolean processPendingUpload(String trackId, FileStateStore.PendingUpload pending,
                                         Map<String, List<LightRagClient.DocumentInfo>> docsByStatus) {
        String foundStatus = null;
        LightRagClient.DocumentInfo foundDoc = null;
        outer:
        for (var statusEntry : docsByStatus.entrySet()) {
            for (LightRagClient.DocumentInfo doc : statusEntry.getValue()) {
                if (trackId.equals(doc.track_id())) {
                    foundStatus = statusEntry.getKey();
                    foundDoc = doc;
                    break outer;
                }
            }
        }

        if (foundDoc == null) {
            handleLostUpload(trackId, pending);
            return false;
        }
        if ("processed".equalsIgnoreCase(foundStatus)) {
            handleProcessedUpload(trackId, pending, foundDoc);
            return true;
        }
        if ("failed".equalsIgnoreCase(foundStatus)) {
            handleFailedUpload(trackId, pending, foundDoc);
            return false;
        }
        return false; // still processing — leave in pendingUploads for next cycle
    }

    private void handleProcessedUpload(String trackId, FileStateStore.PendingUpload pending,
                                       LightRagClient.DocumentInfo doc) {
        log.info("Pending upload processed: {} (docId={})", pending.fileName(), doc.id());
        FileStateStore.FileStateEntry state = store.getEntry(pending.fileName());
        if (state != null) {
            store.putEntry(pending.fileName(),
                    new FileStateStore.FileStateEntry(state.hash(), state.lastModified(), doc.id()));
        }
        store.removePendingUpload(trackId);
    }

    private void handleFailedUpload(String trackId, FileStateStore.PendingUpload pending,
                                    LightRagClient.DocumentInfo doc) {
        String reason = doc.error_msg() != null ? doc.error_msg() : "LightRAG status: failed";
        log.error("Upload failed in LightRAG: {} (trackId={}, reason={})", pending.fileName(), trackId, reason);
        failureLogWriter.logFailure(pending.fileName(), reason, trackId, pending.hash(), doc.created_at());
        if (cleanupFailedDocs) {
            try { lightRagClient.deleteDocument(doc.id()); } catch (Exception ignored) {}
        }
        store.removePendingUpload(trackId);
    }

    private void handleLostUpload(String trackId, FileStateStore.PendingUpload pending) {
        log.warn("Pending upload not found in LightRAG: {} (trackId={})", pending.fileName(), trackId);
        failureLogWriter.logFailure(pending.fileName(), "Document not found in LightRAG after upload",
                trackId, pending.hash(), null);
        store.removePendingUpload(trackId);
    }

    // ── File change handlers ────────────────────────────────────────────

    private boolean handleNewAndUpdatedFiles(Map<String, Long> currentFiles) {
        boolean changed = false;
        for (var entry : currentFiles.entrySet()) {
            String fileName = entry.getKey();
            long lastModified = entry.getValue();
            try {
                FileStateStore.FileStateEntry state = store.getEntry(fileName);
                if (state == null) {
                    log.info("CREATE: {}", fileName);
                    UploadResult result = downloadAndUpload(fileName);
                    store.putEntry(fileName, new FileStateStore.FileStateEntry(result.hash(), lastModified, result.docId()));
                    changed = true;
                } else if (state.lastModified() != lastModified) {
                    log.info("UPDATE: {} (delete + re-upload)", fileName);
                    deleteByDocId(fileName);
                    UploadResult result = downloadAndUpload(fileName);
                    store.putEntry(fileName, new FileStateStore.FileStateEntry(result.hash(), lastModified, result.docId()));
                    changed = true;
                }
            } catch (LightRagBusyException e) {
                log.warn("UPDATE deferred (LightRAG busy): {}", fileName);
            } catch (Exception e) {
                log.error("Failed to process {}: {}", fileName, e.getMessage());
                FileStateStore.FileStateEntry state = store.getEntry(fileName);
                failureLogWriter.logFailure(fileName, e.getMessage(),
                        null, state != null ? state.hash() : null, null);
            }
        }
        return changed;
    }

    private boolean handleDeletedFiles(Set<String> currentFileNames) {
        boolean changed = false;
        var removedFiles = store.getFileNames().stream()
                .filter(name -> !currentFileNames.contains(name))
                .toList();
        for (String fileName : removedFiles) {
            try {
                log.info("DELETE: {}", fileName);
                deleteByDocId(fileName);
                store.removeEntry(fileName);
                changed = true;
            } catch (LightRagBusyException e) {
                log.warn("DELETE deferred (LightRAG busy): {}", fileName);
            } catch (Exception e) {
                log.error("Failed to delete {}: {}", fileName, e.getMessage());
                store.removeEntry(fileName);
                changed = true;
            }
        }
        return changed;
    }

    // ── Upload / Delete helpers ─────────────────────────────────────────

    private UploadResult downloadAndUpload(String fileName) {
        Path tempFile = remoteFileSource.downloadFile(fileName);
        Path processedFile = null;
        try {
            // Hash is computed on the original downloaded file (represents source content)
            String hash = computeFileHash(tempFile);
            if (failureLogWriter.isFileHashFailed(fileName, hash)) {
                log.info("Skipping upload of {} — same content already failed previously", fileName);
                return new UploadResult(hash, null);
            }

            processedFile = preprocessor.process(tempFile, fileName);

            Path uploadFile = processedFile.resolveSibling(fileName);
            Files.move(processedFile, uploadFile, StandardCopyOption.REPLACE_EXISTING);
            processedFile = null; // moved — no separate cleanup needed
            try {
                String trackId = lightRagClient.uploadDocument(uploadFile);
                if (trackId != null) {
                    store.addPendingUpload(trackId, new FileStateStore.PendingUpload(fileName, hash, Instant.now()));
                }
                String docId = resolveDocId(trackId, fileName);
                if (docId != null && trackId != null) {
                    store.removePendingUpload(trackId);
                }
                return new UploadResult(hash, docId);
            } finally {
                Files.deleteIfExists(uploadFile);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare upload for " + fileName, e);
        } finally {
            try { Files.deleteIfExists(tempFile); } catch (Exception ignored) {}
            // Clean up preprocessor output only if it's a different file and wasn't moved yet
            if (processedFile != null && !processedFile.equals(tempFile)) {
                try { Files.deleteIfExists(processedFile); } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Deletes a document from LightRAG.
     * Primary: uses stored docId from state.
     * Fallback (migration): searches by file_path if docId is null.
     */
    private void deleteByDocId(String fileName) {
        FileStateStore.FileStateEntry state = store.getEntry(fileName);
        String docId = state != null ? state.docId() : null;

        if (docId == null) {
            log.warn("No docId in state for {}, skipping delete", fileName);
            return;
        }

        try {
            lightRagClient.deleteDocument(docId);
        } catch (LightRagBusyException e) {
            store.addPendingDelete(docId, new FileStateStore.PendingDelete(fileName, false));
            throw e;
        }
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
                for (LightRagClient.DocumentInfo doc : docsByStatus.getOrDefault("failed", List.of())) {
                    if (trackId.equals(doc.track_id())) {
                        String reason = doc.error_msg() != null ? doc.error_msg() : "LightRAG status: failed";
                        log.error("Upload immediately failed in LightRAG: {} (trackId={}, reason={})",
                                fileName, trackId, reason);
                        FileStateStore.FileStateEntry state = store.getEntry(fileName);
                        failureLogWriter.logFailure(fileName, reason,
                                trackId, state != null ? state.hash() : null, doc.created_at());
                        store.removePendingUpload(trackId);
                        if (cleanupFailedDocs) {
                            try { lightRagClient.deleteDocument(doc.id()); } catch (Exception ignored) {}
                        }
                        return null;
                    }
                }
            }

            List<LightRagClient.DocumentInfo> allDocs = new ArrayList<>();
            docsByStatus.values().forEach(allDocs::addAll);

            // Primary: match by track_id
            if (trackId != null) {
                var match = allDocs.stream().filter(doc -> trackId.equals(doc.track_id())).findFirst();
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

}
