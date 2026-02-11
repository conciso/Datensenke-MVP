# Datensenke MVP

Proof of Concept: Eine Spring-Boot-Anwendung, die ein Remote-Verzeichnis per SFTP oder FTP auf PDF-Dateien ueberwacht und Aenderungen (Create, Update, Delete) automatisch per REST-API an [LightRAG](https://github.com/HKUDS/LightRAG) weitergibt.

## Architektur

```
┌─────────────────┐   SFTP/FTP       ┌──────────────┐
│  Remote-Server  │ ◄──────────────► │  Datensenke  │
│  (PDF-Dateien)  │                  │  (Spring Boot)│
└─────────────────┘                  └──────┬───────┘
                                            │ REST
                                            ▼
                                     ┌──────────────┐
                                     │   LightRAG   │
                                     │   API Server  │
                                     └──────────────┘
```

- **Polling** im konfigurierbaren Intervall (Default: 60s)
- **Protokoll** waehlbar: SFTP (SSH) oder FTP
- **Neue PDF** auf Remote-Server → Download + Upload an LightRAG
- **Geaenderte PDF** (lastModified) → Delete + Re-Upload
- **Geloeschte PDF** → Delete in LightRAG

## Voraussetzungen

- Docker & Docker Compose
- Externes Docker-Netzwerk `ki-playground` (wird von LightRAG mitgenutzt)
- Laufende LightRAG-Instanz im selben Netzwerk
- Erreichbarer SFTP- oder FTP-Server mit PDF-Dateien

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
# .env editieren: API Key, Remote-Server-Zugangsdaten anpassen
```

### 4. Datensenke starten

```bash
docker compose up --build -d
```

### 5. PDFs auf Remote-Server ablegen

Die Datensenke verbindet sich per SFTP/FTP zum konfigurierten Server, erkennt neue PDF-Dateien beim naechsten Polling-Durchlauf und laedt sie an LightRAG hoch.

## Konfiguration

Alle Einstellungen werden ueber eine `.env`-Datei gesteuert (siehe `.env.example`):

| Variable | Default | Beschreibung |
|---|---|---|
| `DATENSENKE_LIGHTRAG_URL` | `http://lightrag:9621` | URL der LightRAG-API |
| `DATENSENKE_LIGHTRAG_API_KEY` | _(leer)_ | API Key fuer LightRAG-Authentifizierung |
| `DATENSENKE_POLL_INTERVAL_MS` | `60000` | Polling-Intervall in Millisekunden |
| `DATENSENKE_REMOTE_PROTOCOL` | `sftp` | Protokoll: `sftp` oder `ftp` |
| `DATENSENKE_REMOTE_HOST` | _(leer)_ | Hostname/IP des Remote-Servers |
| `DATENSENKE_REMOTE_PORT` | `22` | Port (22 fuer SFTP, 21 fuer FTP) |
| `DATENSENKE_REMOTE_USERNAME` | _(leer)_ | Benutzername |
| `DATENSENKE_REMOTE_PASSWORD` | _(leer)_ | Passwort |
| `DATENSENKE_REMOTE_DIRECTORY` | `/documents` | Verzeichnis auf dem Remote-Server |

## Projektstruktur

```
Datensenke-MVP/
├── pom.xml                          # Spring Boot 3.4, Java 21
├── Dockerfile                       # Multi-Stage Build
├── docker-compose.yml               # Datensenke Service
├── LightRAG/
│   └── docker-compose-lightrag.yml  # LightRAG Service
└── src/main/
    ├── java/de/conciso/datensenke/
    │   ├── DatensenkeApplication.java     # Main + @EnableScheduling
    │   ├── FileWatcherService.java        # Polling-Logik
    │   ├── LightRagClient.java            # REST-Client fuer LightRAG
    │   ├── RemoteFileSource.java          # Interface fuer Remote-Zugriff
    │   ├── RemoteFileInfo.java            # Record fuer Datei-Metadaten
    │   ├── RemoteFileSourceConfig.java    # Bean-Konfiguration (SFTP/FTP)
    │   ├── SftpFileSource.java            # SFTP-Implementierung (JSch)
    │   └── FtpFileSource.java             # FTP-Implementierung (Commons Net)
    └── resources/
        └── application.yml                # Default-Konfiguration
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

Dabei die Properties anpassen (`application.yml`) oder per Environment-Variablen uebersteuern, z.B.:

```bash
DATENSENKE_REMOTE_HOST=myserver.local DATENSENKE_REMOTE_USERNAME=user DATENSENKE_REMOTE_PASSWORD=pass ./mvnw spring-boot:run
```

## Technologie-Stack

- Java 21
- Spring Boot 3.4
- Spring RestClient
- JSch (SFTP)
- Apache Commons Net (FTP)
- Docker Multi-Stage Build
- Maven Wrapper
