package de.conciso.datensenke;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
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
    public String uploadDocument(Path file) {
        var body = new MultipartBodyBuilder();
        body.part("file", new FileSystemResource(file));

        var response = restClient.post()
                .uri("/documents/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body.build())
                .retrieve()
                .body(UploadResponse.class);

        String trackId = response != null ? response.track_id() : null;
        log.info("Uploaded document: {} (track_id={})", file.getFileName(), trackId);
        return trackId;
    }

    public Map<String, List<DocumentInfo>> getDocumentsByStatus() {
        var response = restClient.get()
                .uri("/documents")
                .retrieve()
                .body(new ParameterizedTypeReference<DocumentsResponse>() {});

        if (response == null || response.statuses() == null) {
            return Map.of();
        }

        return response.statuses();
    }

    public List<DocumentInfo> getDocuments() {
        List<DocumentInfo> allDocs = new ArrayList<>();
        getDocumentsByStatus().values().forEach(allDocs::addAll);
        return allDocs;
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

    record DocumentsResponse(Map<String, List<DocumentInfo>> statuses) {}

    public record DocumentInfo(String id, String file_path, String created_at, String track_id, String error_msg) {}

    record DeleteDocRequest(List<String> doc_ids) {}

    record DeleteDocResponse(String status, String message, String doc_id) {}
}
