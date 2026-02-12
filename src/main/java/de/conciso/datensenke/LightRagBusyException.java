package de.conciso.datensenke;

/**
 * Thrown when LightRAG responds with status "busy" (pipeline is processing).
 * Callers should retry the operation on the next poll cycle.
 */
public class LightRagBusyException extends RuntimeException {

    public LightRagBusyException(String message) {
        super(message);
    }
}
