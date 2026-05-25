# Technische Setup-Regeln

## Projekt-Typ
- Python Crypto Trading Bot (Bitget Futures)
- Single-File Architektur: olympus.py
- Python 3.9+ kompatibel

## Dependencies
- ccxt (Exchange API)
- aiosqlite (Datenbank)
- numpy (Berechnungen)
- scikit-learn (ML-Modell)
- aiohttp (Webhook/Dashboard)

## Umgebung
- .env Datei für alle API Keys (BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSPHRASE, TELEGRAM_TOKEN, etc.)
- SQLite Datenbank: olympus.db
- Log-Datei: olympus.log

## Code-Stil
- Async/Await für alle I/O Operationen
- Type Hints verwenden
- Funktionale Hilfsfunktionen mit Docstrings
- Formatter: Black (Zeilenlänge 100)
- Import-Sortierung: isort

## Architektur-Entscheidungen
- Alles in einer Datei (bewusste Entscheidung für einfaches Deployment)
- Globale Konfiguration über CFG Dictionary
- Retry-Decorator für Exchange-Calls
- Cache für API-Daten mit TTL
