# 🖥️ Real-Time System Monitoring

> Pipeline complet de surveillance système en temps réel : **Python → Apache Kafka → Google Sheets → Looker Studio**

![Python](https://img.shields.io/badge/Python-3.14-blue)![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-orange)![Google Sheets](https://img.shields.io/badge/Google%20Sheets-API-green)![Looker Studio](https://img.shields.io/badge/Looker%20Studio-Dashboard-yellow)

---

## 📋 Description

Ce projet collecte en temps réel les métriques système (CPU, RAM, Disque, Réseau) via **psutil**, les publie dans **Apache Kafka**, et les stocke dans **Google Sheets** pour une visualisation dans **Looker Studio**.

---

## 🏗️ Architecture

```
psutil (collecte)
      |
      v
monitor.py (Producer)
      |
      +---> Apache Kafka (topic: system-metrics)
      |
      +---> Google Sheets
               |
               +---> TimeSeries Data  (historique complet)
               +---> Last Only        (valeurs en temps réel)
                          |
                          v
                    Looker Studio
                    (Dashboard 2 pages)
```

---

## 📁 Structure du projet

```
real-time-monitoring/
├── config/
│   ├── credentials.json        # Fichier OAuth Google (non versionné)
│   └── token.pickle            # Généré automatiquement
├── diagrams/                   # Schémas du pipeline
├── scripts/
│   ├── monitor.py              # Script principal (Kafka + Google Sheets)
│   ├── heavy_mouse_tracking.py # Suivi souris → Google Sheets
│   ├── heavy_network_tracking.py # Suivi réseau → Google Sheets
│   ├── producer_cpu.py         # Producer Kafka — CPU
│   ├── producer_ram.py         # Producer Kafka — RAM
│   ├── producer_disk.py        # Producer Kafka — Disque
│   └── consumer.py             # Consumer Kafka → Google Sheets
├── requirements.txt
├── setup_project.bat
└── README.md
```

---

## ⚙️ Prérequis

* Python 3.8+
* Java 8+ (requis pour Kafka)
* Apache Kafka 3.x
* Compte Google avec accès Google Cloud Console

---

## 🚀 Installation

### 1. Cloner le projet

```bash
git clone https://github.com/ton-username/real-time-monitoring.git
cd real-time-monitoring
```

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

Ou avec un Python spécifique :

```bash
C:/Python314/python.exe -m pip install -r requirements.txt
```

### 3. Configurer l'API Google

1. Va sur [Google Cloud Console](https://console.cloud.google.com/)
2. Active **Google Sheets API** et **Google Drive API**
3. Crée des identifiants OAuth 2.0 (Application de bureau)
4. Télécharge le fichier JSON → renomme-le `credentials.json`
5. Place-le dans `config/credentials.json`

### 4. Installer et démarrer Kafka

```bash
# Télécharge Kafka depuis https://kafka.apache.org/downloads
# Décompresse dans C:\kafka (Windows) ou /opt/kafka (Linux/macOS)

# Terminal 1 — ZooKeeper
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties

# Terminal 2 — Kafka Broker
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

---

## ▶️ Lancement

```bash
# Script principal (Kafka + Google Sheets)
C:/Python314/python.exe scripts/monitor.py
```

Le script crée automatiquement :

* Le topic Kafka `system-metrics`
* Le Google Sheet "System Monitor"
* Les onglets "TimeSeries Data" et "Last Only"

Sortie console :

```
Authentification Google Sheets...
Google Sheets : connecte !
[Kafka] Connecte au broker localhost:9092 - topic: system-metrics
Monitoring demarre - envoi toutes les 30s. Ctrl+C pour arreter.

[2026-02-20 11:40:10] CPU: 21.2% | RAM: 68.7% | Disk: 97.2% | Kafka: OK
[2026-02-20 11:40:40] CPU: 5.2%  | RAM: 68.5% | Disk: 97.2% | Kafka: OK
```

---

## 📊 Métriques collectées

| Métrique       | Description                       | Unité |
| --------------- | --------------------------------- | ------ |
| CPU%            | Utilisation globale du processeur | %      |
| CPU per Core    | Utilisation par cœur logique     | %      |
| RAM%            | Utilisation de la mémoire vive   | %      |
| RAM Used GB     | Mémoire utilisée                | GB     |
| Swap%           | Utilisation de la mémoire swap   | %      |
| Disk%           | Espace disque utilisé            | %      |
| Disk Read KB/s  | Débit de lecture disque          | KB/s   |
| Disk Write KB/s | Débit d'écriture disque         | KB/s   |
| Net Sent KB/s   | Débit réseau envoyé            | KB/s   |
| Net Recv KB/s   | Débit réseau reçu              | KB/s   |

---

## 📈 Dashboard Looker Studio

### Page 1 — Historique (TimeSeries Data)

* CPU Usage Over Time (courbe rouge)
* RAM Usage Over Time (courbe bleue)
* Disk Usage Over Time (courbe orange)
* Network Sent KB/s (courbe cyan)
* Network Recv KB/s (courbe verte)
* Disk Read / Write KB/s

### Page 2 — Temps réel (Last Only)

* Scorecards : CPU%, RAM%, Swap%, RAM Used GB
* Scorecards : Disk Read/Write KB/s, Net Sent/Recv KB/s
* Jauge Disk% (vert/orange/rouge)
* Indicateur CPU Status (NORMAL / MEDIUM / HIGH)
* Timestamp dernière mise à jour

---

## 🔧 Configuration

| Variable             | Valeur par défaut | Description                  |
| -------------------- | ------------------ | ---------------------------- |
| `INTERVAL`         | `30`             | Secondes entre chaque mesure |
| `KAFKA_BROKER`     | `localhost:9092` | Adresse du broker Kafka      |
| `KAFKA_TOPIC`      | `system-metrics` | Nom du topic Kafka           |
| `SPREADSHEET_NAME` | `System Monitor` | Nom du Google Sheet          |

---

## 🐛 Dépannage

| Erreur                           | Solution                                                 |
| -------------------------------- | -------------------------------------------------------- |
| `ModuleNotFoundError: kafka`   | `C:/Python314/python.exe -m pip install kafka-python`  |
| `NoBrokersAvailable`           | Démarre ZooKeeper et Kafka avant le script              |
| `Error 403: access_denied`     | Ajoute ton email comme testeur dans Google Cloud Console |
| `APIError 429: Quota exceeded` | Augmente `INTERVAL`à 30s minimum                      |
| `DeprecationWarning: utcnow()` | Déjà corrigé dans la dernière version                |
| `token.pickle`expiré          | Supprime `config/token.pickle`et relance               |

---

## 📦 Dépendances

```
psutil>=5.9.0
gspread>=5.10.0
google-auth>=2.17.0
google-auth-oauthlib>=1.0.0
google-auth-httplib2>=0.2.0
pynput>=1.7.6
kafka-python>=2.0.2
```

---

## 🔒 Sécurité

Le fichier `.gitignore` exclut automatiquement les fichiers sensibles :

```
config/credentials.json
config/token.pickle
```

> Ne commitez jamais ces fichiers sur GitHub.

---

## 👤 Auteur

**Ayoub El Harem**

* Lien vers lookerstudio: [@lookerstudio](https://lookerstudio.google.com/reporting/f2b4c6fe-5f36-40f7-8e82-d705fd878bc4)

---

## 📄 Licence

Ce projet est réalisé dans un cadre pédagogique.
