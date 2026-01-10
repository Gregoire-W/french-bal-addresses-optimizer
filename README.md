# Projet Spark - Gestion des Adresses BAL (Base d'Adresses Locales)

Ce projet implémente une solution automatisée avec Apache Spark pour sauvegarder et gérer les fichiers d'adresses françaises de manière incrémentale, permettant une rétention à long terme (>= 30 ans) sans coût excessif.

## Architecture

Le projet utilise une approche incrémentale :
- **bal_latest/** : contient le snapshot le plus récent complet
- **bal_diff/** : contient les différences journalières partitionnées par jour (INSERT/UPDATE/DELETE)

Cette approche permet de stocker 30 ans de données de manière très efficace :
- Au lieu de 21 Po (30 ans × 365 jours × 2 Go), nous stockons seulement les changements quotidiens
- Les changements quotidiens sont typiquement très petits (quelques Mo au lieu de 2 Go)

## Prérequis

- Java 11
- Maven 3.x
- Apache Spark 3.5.0
- Docker (optionnel, pour l'environnement de développement)

## Compilation

```bash
mvn clean install
```

Cela génère le fichier JAR : `target/spark-project-1.0-SNAPSHOT.jar`

## Les 4 Jobs Implémentés

### 1. Job d'Intégration Quotidienne

**Script** : `scripts/run_daily_file_integration.sh`

**Usage** :
```bash
./scripts/run_daily_file_integration.sh <date> <csvFile>
```

**Exemple** :
```bash
./scripts/run_daily_file_integration.sh 2025-01-01 /path/to/adresses-france.csv
```

**Fonctionnement** :
1. Lit le fichier CSV du jour (format BAL avec séparateur `;`)
2. Compare avec `bal_latest` (s'il existe)
3. Calcule les différences :
   - **INSERT** : nouvelles adresses
   - **UPDATE** : adresses modifiées
   - **DELETE** : adresses supprimées
4. Sauvegarde les diffs dans `bal.db/bal_diff` partitionné par jour
5. Met à jour `bal.db/bal_latest` avec les données courantes

### 2. Rapport Quotidien

**Script** : `scripts/run_report.sh`

**Usage** :
```bash
./scripts/run_report.sh
```

**Fonctionnement** :
- Lit `bal_latest`
- Affiche des statistiques agrégées :
  - Nombre total d'adresses
  - Nombre d'adresses par département
  - Top 10 des départements
  - Statistiques de distribution

### 3. Reconstruction d'un Dump à une Date

**Script** : `scripts/recompute_and_extract_dump_at_date.sh`

**Usage** :
```bash
./scripts/recompute_and_extract_dump_at_date.sh <date> <outputDir>
```

**Exemple** :
```bash
./scripts/recompute_and_extract_dump_at_date.sh 2025-01-24 /output/dump-2025-01-24
```

**Fonctionnement** :
1. Lit tous les diffs depuis le premier jour jusqu'à la date cible
2. Reconstruit progressivement l'état en appliquant chaque diff :
   - Applique les DELETE (retire les adresses)
   - Applique les UPDATE (remplace les adresses)
   - Applique les INSERT (ajoute les adresses)
3. Sauvegarde le dump complet en Parquet dans le répertoire de sortie

### 4. Calcul de Différences entre Deux Dumps

**Script** : `scripts/compute_diff_between_files.sh`

**Usage** :
```bash
./scripts/compute_diff_between_files.sh <parquetDir1> <parquetDir2>
```

**Exemple** :
```bash
./scripts/compute_diff_between_files.sh /temp/dumpA /temp/dumpB
```

**Fonctionnement** :
- Compare deux datasets Parquet
- Affiche :
  - Adresses supprimées (dans 1, pas dans 2)
  - Adresses ajoutées (dans 2, pas dans 1)
  - Adresses modifiées (dans les deux mais avec contenu différent)
  - Adresses inchangées
  - Statistiques de synthèse

## Structure des Données

### Format CSV d'entrée

Le fichier CSV utilise `;` comme séparateur et contient les colonnes :
- `uid_adresse` : Identifiant unique de l'adresse
- `cle_interop` : Clé d'interopérabilité
- `commune_insee` : Code INSEE de la commune
- `commune_nom` : Nom de la commune
- `voie_nom` : Nom de la voie
- `numero` : Numéro dans la voie
- `lat`, `long` : Coordonnées géographiques
- Et autres champs...

### Structure de stockage

```
bal.db/
├── bal_latest/          # Snapshot le plus récent (Parquet)
│   └── *.parquet
└── bal_diff/            # Diffs quotidiens partitionnés
    ├── day=2025-01-01/
    │   └── *.parquet
    ├── day=2025-01-02/
    │   └── *.parquet
    └── ...
```

Chaque fichier dans `bal_diff` contient :
- Toutes les colonnes de l'adresse
- `operation` : INSERT, UPDATE ou DELETE
- `day` : Date du changement (utilisé pour le partitionnement)

## Test d'Intégration

Pour tester avec 50 jours de données (2025-01-01 à 2025-02-20) :

```bash
#!/bin/bash

for n in $(seq 0 49); do 
    day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
    echo "Processing day: ${day}"
    ./scripts/run_daily_file_integration.sh ${day} /path/to/dump-${day}.csv
    ./scripts/run_report.sh
done

# Reconstruction de dumps à des dates spécifiques
./scripts/recompute_and_extract_dump_at_date.sh 2025-01-24 /temp/dumpA
./scripts/recompute_and_extract_dump_at_date.sh 2025-02-10 /temp/dumpB

# Comparaison des deux dumps
./scripts/compute_diff_between_files.sh /temp/dumpA /temp/dumpB
```

## Utilisation avec Docker

Si vous utilisez l'environnement Docker fourni :

```bash
# Démarrer le conteneur
docker compose up -d

# Entrer dans le conteneur
docker exec -it spark_project_env bash

# Dans le conteneur, compiler le projet
cd /app
mvn clean install

# Exécuter les jobs
./scripts/run_daily_file_integration.sh 2025-01-01 data/adresses-02.csv
./scripts/run_report.sh
```

## Exécution Manuelle avec spark-submit

Vous pouvez aussi exécuter les jobs manuellement :

```bash
spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  target/spark-project-1.0-SNAPSHOT.jar \
  <command> [args...]
```

Où `<command>` peut être :
- `integration <date> <csvFile>`
- `report`
- `recompute <date> <outputDir>`
- `diff <parquetDir1> <parquetDir2>`

## Optimisations

Le projet utilise plusieurs optimisations Spark :
- **Cache** : Les datasets fréquemment utilisés sont mis en cache
- **Partitionnement** : Les diffs sont partitionnés par jour pour des lectures efficaces
- **Parquet** : Format columnar compressé pour un stockage optimal
- **Left Anti Join** : Pour les opérations de différence (très efficace)

## Extensibilité

Le code est structuré pour être facilement extensible :
- Ajouter de nouvelles colonnes dans les comparaisons
- Modifier la logique de détection des changements
- Ajouter de nouveaux jobs dans `SparkMain.java`
- Personnaliser les rapports

## Auteur

Projet réalisé dans le cadre du cours Spark à l'ESILV.
