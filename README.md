# Datensenke MVP

Proof of Concept: Eine Spring-Boot-Anwendung, die ein Verzeichnis (lokal oder Netzlaufwerk) auf PDF-Dateien ueberwacht und Aenderungen (Create, Update, Delete) automatisch per REST-API an [LightRAG](https://github.com/HKUDS/LightRAG) weitergibt.

## Architektur

```
┌─────────────────┐     Polling      ┌──────────────┐
│  Watch-Verz.    │ ◄──────────────► │  Datensenke  │
│  (Volume Mount) │                  │  (Spring Boot)│
└─────────────────┘                  └──────┬───────┘
                                            │ REST
                                            ▼
                                     ┌──────────────┐
                                     │   LightRAG   │
                                     │   API Server  │
                                     └──────────────┘
```

- **Polling** im konfigurierbaren Intervall (Default: 60s)
- **Neue PDF** im Verzeichnis → Upload an LightRAG
- **Geaenderte PDF** (lastModified) → Delete + Re-Upload
- **Geloeschte PDF** → Delete in LightRAG

## Voraussetzungen

- Docker & Docker Compose
- Externes Docker-Netzwerk `ki-playground` (wird von LightRAG mitgenutzt)
- Laufende LightRAG-Instanz im selben Netzwerk

## Schnellstart

### 1. Docker-Netzwerk erstellen (einmalig)

```bash
docker network create ki-playground
```

### 2. LightRAG starten

```bash
docker compose -f LightRAG/docker-compose-lightrag.yml up -d
```

LightRAG WebUI ist erreichbar unter: http://localhost:9622

### 3. Konfiguration anlegen

```bash
cp .env.example .env
# .env editieren: API Key und Dokumentenpfad anpassen
```

### 4. Datensenke starten

```bash
docker compose up --build -d
```

### 5. PDFs ablegen

```bash
cp mein-dokument.pdf documents/
```

Die Datensenke erkennt die Datei beim naechsten Polling-Durchlauf und laedt sie an LightRAG hoch.

## Konfiguration

Alle Einstellungen werden ueber eine `.env`-Datei gesteuert (siehe `.env.example`):

| Variable | Default | Beschreibung |
|---|---|---|
| `DATENSENKE_DOCUMENTS_PATH` | `./documents` | Pfad zum Dokumentenverzeichnis (lokal oder Netzlaufwerk/NFS-Mount) |
| `DATENSENKE_LIGHTRAG_URL` | `http://lightrag:9621` | URL der LightRAG-API |
| `DATENSENKE_LIGHTRAG_API_KEY` | _(leer)_ | API Key fuer LightRAG-Authentifizierung |
| `DATENSENKE_POLL_INTERVAL_MS` | `60000` | Polling-Intervall in Millisekunden |

### Remote-/Netzwerkverzeichnis einbinden

Um ein Netzlaufwerk (NFS, SMB/CIFS) zu ueberwachen, muss dieses auf dem Docker-Host gemountet sein. Den Mount-Pfad dann in `.env` angeben:

```bash
DATENSENKE_DOCUMENTS_PATH=/mnt/nfs/shared-documents
```

## Projektstruktur

```
Datensenke-MVP/
├── pom.xml                          # Spring Boot 3.4, Java 21
├── Dockerfile                       # Multi-Stage Build
├── docker-compose.yml               # Datensenke Service
├── documents/                       # Watch-Verzeichnis (Volume Mount)
├── LightRAG/
│   └── docker-compose-lightrag.yml  # LightRAG Service
└── src/main/
    ├── java/de/conciso/datensenke/
    │   ├── DatensenkeApplication.java   # Main + @EnableScheduling
    │   ├── FileWatcherService.java      # Polling-Logik
    │   └── LightRagClient.java          # REST-Client fuer LightRAG
    └── resources/
        └── application.yml              # Default-Konfiguration
```

## Logs pruefen

```bash
docker compose logs -f datensenke
```

Erwartete Log-Ausgaben:

```
CREATE: dokument.pdf
UPDATE: dokument.pdf (delete + re-upload)
DELETE: dokument.pdf
```

## Lokale Entwicklung

```bash
./mvnw spring-boot:run
```

Dabei die Properties anpassen (`application.yml`) oder per Environment-Variablen uebersteurn, z.B.:

```bash
DATENSENKE_WATCH_DIRECTORY=./documents DATENSENKE_LIGHTRAG_URL=http://localhost:9622 ./mvnw spring-boot:run
```

## Technologie-Stack

- Java 21
- Spring Boot 3.4
- Spring RestClient
- Docker Multi-Stage Build
- Maven Wrapper
