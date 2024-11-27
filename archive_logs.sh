#!/bin/bash

# Variables
LOG_FILE="logs/extraction.log"
ARCHIVE_DIR="archive_logs"
SCHEDULE_LOG="logs/schedule.log"
MAX_LOG_SIZE=$((5 * 1024 * 1024))  # Taille maximale du fichier log avant archivage (5 MB)

# Création du dossier d'archive s'il n'existe pas
if [ ! -d "$ARCHIVE_DIR" ]; then
    mkdir -p "$ARCHIVE_DIR"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Création du dossier d'archive : $ARCHIVE_DIR" >> "$SCHEDULE_LOG"
fi

# Vérification de la taille du fichier log
if [ -f "$LOG_FILE" ] && [ $(stat --format="%s" "$LOG_FILE") -ge "$MAX_LOG_SIZE" ]; then
    TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
    ARCHIVE_FILE="$ARCHIVE_DIR/extraction_$TIMESTAMP.log"

    # Archiver le fichier log
    mv "$LOG_FILE" "$ARCHIVE_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Fichier log archivé : $ARCHIVE_FILE" >> "$SCHEDULE_LOG"

    # Compresser l'archive
    gzip "$ARCHIVE_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Archive compressée : $ARCHIVE_FILE.gz" >> "$SCHEDULE_LOG"

    # Réinitialiser le fichier log
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Nouveau fichier log initialisé après archivage." > "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Pas de fichier log à archiver (taille inférieure à $MAX_LOG_SIZE bytes)." >> "$SCHEDULE_LOG"
fi
