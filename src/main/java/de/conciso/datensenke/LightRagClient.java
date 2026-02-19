package de.conciso.datensenke;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class LightRagClient {

    private static final Logger log = LoggerFactory.getLogger(LightRagClient.class);

    private final RestClient restClient;

    public LightRagClient(
            @Value("${datensenke.lightrag-url}") String lightragUrl,
            @Value("${datensenke.lightrag-api-key:}") String apiKey) {
        var builder = RestClient.builder().baseUrl(lightragUrl);
        if (apiKey != null && !apiKey.isBlank()) {
            builder.defaultHeader("X-API-Key", apiKey);
            log.info("LightRAG API key configured");
        }
        this.restClient = builder.build();
    }

    /**
     * Uploads a document and returns the track_id from the LightRAG response.
     */
    public record UploadAttempt(String trackId, String status, String message) {
        public boolean isAccepted() { return trackId != null && !trackId.isBlank(); }
    }

    public UploadAttempt uploadDocument(Path file) {
        var body = new MultipartBodyBuilder();
        body.part("file", new FileSystemResource(file));

        var response = restClient.post()
                .uri("/documents/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body.build())
                .retrieve()
                .body(UploadResponse.class);

        String trackId = response != null ? response.track_id() : null;
        String status  = response != null ? response.status()   : null;
        String message = response != null ? response.message()  : null;

        if (trackId == null || trackId.isBlank()) {
            log.warn("Uploaded document: {} — LightRAG rejected upload (status={}, message={})",
                    file.getFileName(), status, message);
        } else {
            log.info("Uploaded document: {} (track_id={})", file.getFileName(), trackId);
        }
        return new UploadAttempt(trackId, status, message);
    }

    /**
     * Returns all documents grouped by their (lowercased) status.
     * Uses the paginated endpoint internally to avoid the 1000-document cap.
     */
    public Map<String, List<DocumentInfo>> getDocumentsByStatus() {
        return fetchAllDocuments(null).stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        d -> d.status() != null ? d.status().toLowerCase() : "unknown"));
    }

    /**
     * Returns all documents regardless of status.
     * Uses the paginated endpoint internally to avoid the 1000-document cap.
     */
    public List<DocumentInfo> getDocuments() {
        return fetchAllDocuments(null);
    }

    /**
     * Fetches all documents via {@code POST /documents/paginated}, following pages until done.
     *
     * @param statusFilter optional LightRAG status string (e.g. "FAILED"); {@code null} fetches all
     */
    private List<DocumentInfo> fetchAllDocuments(String statusFilter) {
        List<DocumentInfo> all = new ArrayList<>();
        int page = 1;
        boolean hasNext = true;
        while (hasNext) {
            var request = new DocumentsRequest(page, 100, "desc", "updated_at", statusFilter);
            var response = restClient.post()
                    .uri("/documents/paginated")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(PaginatedDocsResponse.class);
            if (response == null || response.documents() == null || response.documents().isEmpty()) break;
            all.addAll(response.documents());
            hasNext = response.pagination() != null && response.pagination().has_next();
            page++;
        }
        return all;
    }

    /**
     * Deletes a document from LightRAG.
     *
     * @throws LightRagBusyException if LightRAG is currently processing and cannot delete
     */
    public void deleteDocument(String docId) {
        var response = restClient.method(org.springframework.http.HttpMethod.DELETE)
                .uri("/documents/delete_document")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new DeleteDocRequest(List.of(docId)))
                .retrieve()
                .body(DeleteDocResponse.class);

        if (response != null && "busy".equals(response.status())) {
            throw new LightRagBusyException(
                    "LightRAG is busy, delete of " + docId + " deferred (message: " + response.message() + ")");
        }

        log.info("Deleted document with id: {} (status: {})", docId,
                response != null ? response.status() : "unknown");
    }

    record UploadResponse(String status, String message, String track_id) {}

    record DocumentsRequest(int page, int page_size, String sort_direction, String sort_field, String status_filter) {}

    record PaginatedDocsResponse(List<DocumentInfo> documents, PaginationInfo pagination) {}

    record PaginationInfo(boolean has_next, boolean has_prev, int page, int page_size, int total_count, int total_pages) {}

    public record DocumentInfo(String id, String file_path, String created_at, String track_id, String status, String error_msg) {}

    record DeleteDocRequest(List<String> doc_ids) {}

    record DeleteDocResponse(String status, String message, String doc_id) {}
}
