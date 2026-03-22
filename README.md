Blissydah — Data Engineering & Stock Analytics Pipeline

## Quick Start (Git/Clean Repo)

This repository is the clean source version (no generated outputs, no secrets).

1. Copy environment templates:

```bash
cp .env.example .env
cp config/db.env.example config/db.env
```

2. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Run a job (module mode):

```bash
python -m jobs.job_02_upsert_ref_p_product_filtered_api_2
```
1. Objectif du projet

Ce projet vise à mettre en place une chaîne de traitement de données fiable, automatisée et audit-ready autour des données opérationnelles issues d’Odoo, avec un focus particulier sur :

la fiabilisation des stocks

la traçabilité des mouvements

la détection proactive des anomalies

la préparation à l’automatisation des contrôles internes

l’industrialisation progressive des traitements (ETL)

L’objectif final est de fournir :

une confiance dans les chiffres

une réduction des pertes

une visibilité managériale

une sérénité opérationnelle

2. Architecture générale du projet
Blissydah/
├── jobs/           # Scripts batch (ETL / extraction / calculs)
├── scripts/        # Librairies communes (clients, helpers, utils)
├── sql/            # Scripts SQL (DDL, vues, contrôles)
├── config/         # Fichiers de configuration (.env, paramètres)
├── daily_reports/  # Fichiers générés (CSV, exports)
├── logs/           # Logs d’exécution
├── venv/           # Environnement virtuel Python
└── README.md

3. Principe de fonctionnement global

La chaîne de traitement repose sur les étapes suivantes :

Extraction des données depuis Odoo (API OdooRPC)

Normalisation et filtrage des données

Chargement dans PostgreSQL (staging → core)

Calculs analytiques :

stocks opening

mouvements journaliers

stocks closing

Contrôles automatisés :

stocks négatifs

incohérences de valorisation

écarts entre référentiels

Génération de rapports (CSV, mails, dashboards à venir)

4. Convention de nommage des jobs

Les scripts dans le dossier jobs/ suivent une numérotation logique, reflétant la séquence ETL.

Job	Description
job_01	Initialisation / prérequis
job_02	Chargement référentiel produits
job_03	Chargement référentiel localisations
job_04	Calendrier & dimensions temps
job_05	Extraction stock (snapshot)
job_06	Extraction des mouvements de stock (stock.move.line)
job_07	Agrégation des mouvements journaliers
job_08	Calcul des stocks closing
job_09	Contrôles & reporting (stocks négatifs, anomalies)

👉 Les jobs sont idempotents autant que possible (upsert, recalcul contrôlé).

5. Détail du Job 06 — Extraction des mouvements de stock
🎯 Objectif

Extraire les mouvements de stock détaillés depuis Odoo (stock.move.line) sur une période glissante (ex. 7 jours), afin de :

tracer les entrées et sorties réelles

préparer les agrégations journalières

alimenter les calculs de stock opening / closing

📥 Source

Odoo (via OdooRPC)

Modèle : stock.move.line

📤 Cible

PostgreSQL

Table : core.fct_sm_stock_movement

⏱️ Fenêtre temporelle

Par défaut : 7 derniers jours

Paramétrable dans le script

🔄 Mode de chargement

Insertion / upsert contrôlé

Prévu pour des exécutions quotidiennes ou infra-journalières

6. Exécution des scripts (IMPORTANT)
✅ Bonne pratique (recommandée)

Les scripts doivent être exécutés depuis la racine du projet en mode module :

python -m jobs.job_06_extract_sm_move_line_7d_1


👉 Cela garantit que :

les imports sont résolus correctement

la structure du projet est respectée

le comportement est stable en production / cron

❌ À éviter
python jobs/job_06_extract_sm_move_line_7d_1.py


Cette commande peut échouer selon le contexte (PYTHONPATH).

7. Gestion des imports

Les librairies communes sont centralisées dans scripts/.

Exemple :

from scripts.odoo_client_odoorpc_fixed import OdooClient


Le dossier scripts/ est un package Python explicite (__init__.py présent).

8. Configuration & variables d’environnement

Les paramètres sensibles sont externalisés dans :

config/
└── db.env


Variables typiques :

DB_HOST
DB_PORT
DB_NAME
DB_USER
DB_PASSWORD
ODOO_URL
ODOO_DB
ODOO_USER
ODOO_SECRET (préféré)
ODOO_API_KEY (alias compatible)
BLISSYDAH_DB_PASSWORD (préféré)


⚠️ Aucun secret ne doit être commité. Les fichiers `.env` et `config/db.env` sont ignorés par git.

9. Logs & traçabilité

Chaque job :

affiche un résumé d’exécution

journalise les volumes traités

permet un diagnostic rapide en cas d’échec

Les logs peuvent être redirigés vers :

logs/

ou un outil de supervision ultérieur

10. État actuel & perspectives
✔️ Déjà en place

Extraction automatisée

Référentiels normalisés

Contrôles de stocks négatifs

Reporting CSV & email

🚀 Prochaines étapes

Orchestration (cron → Airflow)

Dashboards managériaux

KPI de maturité data & contrôle interne

Extension vers ventes, trésorerie, analytics avancé

11. Philosophie du projet

Mieux vaut une donnée fiable aujourd’hui qu’un dashboard faux demain.

Ce projet privilégie :

la rigueur

la traçabilité

la compréhension métier

l’automatisation progressive
