# Moneyplot

Plateforme personnelle de suivi des prix immobiliers en France, basée sur les transactions réelles (DVF) et enrichie avec des indicateurs macro-économiques.

## Pourquoi

Éclairer des décisions d'achat immobilier en centralisant et croisant plusieurs sources de données ouvertes : prix réels, taux d'intérêt, indices de marché, diagnostics énergétiques.

## Stack

| Couche | Outil | Rôle |
|--------|-------|------|
| Stockage | DuckDB | Base analytique colonnaire (fichier unique) |
| Orchestration | Dagster | Pipeline data centré sur les assets |
| Dashboard | Streamlit | Visualisation interactive (cartes, graphiques) |
| Packages | uv | Gestionnaire de dépendances Python |

## Quickstart

```bash
# Installer les dépendances
uv sync

# Lancer le pipeline (téléchargement + nettoyage + chargement)
uv run dagster dev -m moneyplot.pipelines.definitions
# → Ouvrir http://localhost:3000, matérialiser les assets

# Lancer le dashboard
uv run streamlit run src/moneyplot/dashboard/app.py
```

## Sources de données

### DVF — Demandes de Valeurs Foncières

Toutes les transactions immobilières réelles depuis 2019 (prix, surface, localisation, type de bien). CSV géolocalisés publiés par Etalab, un fichier par département.

- **URL** : `https://files.data.gouv.fr/geo-dvf/latest/csv/`
- **Fréquence** : semestrielle (avril + octobre)
- **Couverture** : France entière sauf Alsace (67, 68), Moselle (57), Mayotte (976)

### Indices Notaires-INSEE

Indices trimestriels d'évolution des prix par type de bien et zone géographique, via l'API INSEE BDM.

| Série | Type de bien | Zone |
|-------|-------------|------|
| `010567006` | Appartements | France |
| `010567007` | Maisons | France |
| `010567008` | Appartements | Île-de-France |
| `010567009` | Appartements | Province |

### Taux hypothécaires BCE

Taux d'intérêt mensuels pour les crédits immobiliers en France (série ECB `MIR.M.FR.B.A2C.A.C.A.2250.EUR.N`).

### DPE — Diagnostics de Performance Énergétique

Classe énergie (A-G) par logement via l'API ADEME. Permet de mesurer l'impact des passoires thermiques sur les prix.

## Structure du projet

```
moneyplot/
├── pyproject.toml
├── data/                           # gitignored
│   ├── raw/dvf/                    # CSV bruts Etalab
│   ├── processed/                  # Parquet nettoyés
│   └── moneyplot.duckdb            # Base analytique
│
├── src/moneyplot/
│   ├── ingestion/                  # Téléchargement des sources
│   │   ├── dvf.py                  # DVF géolocalisé Etalab
│   │   ├── insee.py                # Indices prix Notaires-INSEE
│   │   ├── ecb.py                  # Taux hypothécaires BCE
│   │   └── dpe.py                  # Diagnostics énergie ADEME
│   │
│   ├── transform/                  # Nettoyage et enrichissement
│   │   ├── dvf_clean.py            # Dédoublonnage, prix/m², export Parquet
│   │   └── enrich.py               # Jointure DVF × DPE
│   │
│   ├── storage/                    # Couche base de données
│   │   ├── db.py                   # Connexion DuckDB
│   │   └── schemas.py              # Création des tables
│   │
│   ├── pipelines/                  # Orchestration Dagster
│   │   ├── definitions.py          # Point d'entrée Dagster
│   │   ├── assets.py               # Assets DVF + macro
│   │   ├── resources.py            # Ressource DuckDB partagée
│   │   └── schedules.py            # Planification
│   │
│   └── dashboard/                  # Interface Streamlit
│       ├── app.py                  # Point d'entrée + sidebar
│       └── pages/
│           ├── 01_carte.py         # Carte des prix par commune
│           ├── 02_evolution.py     # Courbes d'évolution temporelle
│           └── 03_compare.py       # Comparaison de communes
│
├── tests/
└── notebooks/
```

## Pipeline Dagster

Le pipeline est organisé en deux groupes d'assets :

```
Groupe DVF :    raw_dvf → cleaned_dvf → dvf_in_duckdb
Groupe Macro :  price_indices    mortgage_rates
```

### Assets

| Asset | Description |
|-------|-------------|
| `raw_dvf` | Télécharge les CSV DVF par département depuis Etalab |
| `cleaned_dvf` | Filtre aux ventes, dédoublonne les mutations, calcule le prix/m², exporte en Parquet |
| `dvf_in_duckdb` | Charge le Parquet nettoyé dans la table `mutations` |
| `price_indices` | Récupère les indices Notaires-INSEE et les charge dans `indices_prix` |
| `mortgage_rates` | Récupère les taux BCE et les charge dans `taux_hypothecaires` |

### Schedules

| Schedule | Cible | Cron | Raison |
|----------|-------|------|--------|
| `dvf_monthly` | `dvf_in_duckdb` | `0 3 1 * *` | DVF mis à jour 2×/an, vérification mensuelle |
| `macro_quarterly` | `price_indices`, `mortgage_rates` | `0 4 1 1,4,7,10 *` | Données trimestrielles |

### Configuration

L'asset `raw_dvf` accepte un paramètre `departments` (liste de codes). Par défaut, tous les départements sont téléchargés.

Pour ne télécharger qu'un sous-ensemble (utile pour tester) :

```python
# Dans Dagit → Launchpad, config YAML :
ops:
  raw_dvf:
    config:
      departments: ["75", "92", "93"]
```

## Dashboard

Trois pages accessibles depuis la barre latérale :

### Carte des prix

Carte interactive (pydeck) affichant le prix médian au m² par commune. Les bulles sont dimensionnées par le nombre de transactions et colorées du vert (bas) au rouge (élevé).

**Filtres** : département, type de bien, année.

### Évolution temporelle

Courbes (plotly) du prix médian au m² par trimestre et département. Un second graphique affiche le volume de transactions. Si les taux hypothécaires sont chargés, un troisième graphique superpose prix et taux sur un axe double.

**Filtres** : départements (multi-sélection), type de bien.

### Comparaison de communes

Sélection de 1 à 5 communes pour une comparaison côte à côte :
- Indicateurs clés (prix médian, surface médiane, nombre de transactions)
- Évolution comparée par trimestre
- Distribution des prix au m² (histogrammes superposés)

**Filtres** : type de bien.

## Schéma DuckDB

La base `data/moneyplot.duckdb` contient 5 tables :

### `mutations`

Table principale des transactions DVF nettoyées.

| Colonne | Type | Description |
|---------|------|-------------|
| `id_mutation` | VARCHAR | Identifiant de la mutation |
| `date_mutation` | DATE | Date de la vente |
| `valeur_fonciere` | DOUBLE | Prix de vente (€) |
| `code_departement` | VARCHAR | Code département |
| `code_commune` | VARCHAR | Code INSEE commune |
| `nom_commune` | VARCHAR | Nom de la commune |
| `type_local` | VARCHAR | Maison ou Appartement |
| `surface_reelle_bati` | DOUBLE | Surface bâtie (m²) |
| `nombre_pieces` | INTEGER | Nombre de pièces |
| `surface_terrain` | DOUBLE | Surface terrain (m²) |
| `longitude` / `latitude` | DOUBLE | Coordonnées GPS |
| `prix_m2` | DOUBLE | Prix au m² calculé |
| `annee` / `trimestre` | INTEGER | Période extraite de la date |

### `indices_prix`

Indices trimestriels Notaires-INSEE (`date`, `indice`, `type_bien`, `zone`).

### `taux_hypothecaires`

Taux mensuels BCE (`date`, `taux`, `source`).

### `communes`

Référentiel géographique et démographique (`code_commune` PK, `population`, `revenu_median`, coordonnées).

### `dpe`

Diagnostics de performance énergétique (`classe_energie`, `classe_ges`, `annee_construction`, `surface_habitable`).

## Développement

```bash
# Installer les dépendances
uv sync

# Lancer les tests
uv run pytest

# Vérifier le code
uv run ruff check src/

# Ouvrir un notebook d'exploration
uv run jupyter notebook notebooks/
```

### Nettoyage DVF — détail

Le processus de nettoyage (`transform/dvf_clean.py`) applique les règles suivantes :
1. **Filtre** : uniquement les ventes (`nature_mutation = 'Vente'`)
2. **Types de bien** : Maison et Appartement uniquement
3. **Montant** : entre 0 € et 10 M€
4. **Dédoublonnage** : par `id_mutation` + `type_local`, conservation de la ligne avec la plus grande surface
5. **Prix/m²** : `valeur_fonciere / surface_reelle_bati` (NULL si surface = 0)
6. **Format de sortie** : Parquet compressé ZSTD
