package de.conciso.datensenke;

import java.nio.file.Path;
import java.util.List;

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

    public LightRagClient(@Value("${datensenke.lightrag-url}") String lightragUrl) {
        this.restClient = RestClient.builder()
                .baseUrl(lightragUrl)
                .build();
    }

    public void uploadDocument(Path file) {
        var body = new MultipartBodyBuilder();
        body.part("file", new FileSystemResource(file));

        restClient.post()
                .uri("/documents/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body.build())
                .retrieve()
                .toBodilessEntity();

        log.info("Uploaded document: {}", file.getFileName());
    }

    public List<DocumentInfo> getDocuments() {
        var response = restClient.get()
                .uri("/documents")
                .retrieve()
                .body(new ParameterizedTypeReference<List<DocumentInfo>>() {});

        return response != null ? response : List.of();
    }

    public void deleteDocument(String docId) {
        restClient.post()
                .uri("/documents/delete")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new DeleteDocRequest(List.of(docId)))
                .retrieve()
                .toBodilessEntity();

        log.info("Deleted document with id: {}", docId);
    }

    public record DocumentInfo(String id, String file_path) {}

    record DeleteDocRequest(List<String> doc_ids) {}
}
