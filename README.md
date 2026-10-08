# TP4 — De Cassandra à Spark : analyse de la saison F1 2024

M2 Big Data & IA — Données distribuées · Cassandra · PySpark · Jupyter · Spark UI

## Sujet

Ce projet reprend les données Formule 1 chargées dans Cassandra lors des TP2 et TP3, issues de l'API [OpenF1](https://openf1.org). Ici, Spark les lit et les traite pour analyser la saison 2024.

**Question métier : quels pilotes et quelles écuries ont dominé la saison 2024, et avec quelle régularité (podiums, abandons) ?**

## Données

| | |
|---|---|
| Cluster | 3 nœuds Cassandra 4.1 (`cass1`, `cass2`, `cass3`) sur le réseau Docker `cass-net` |
| Keyspace | `f1` — `NetworkTopologyStrategy`, `dc1 = 3` |
| Table | `results_by_driver` — 479 lignes : résultat de chaque pilote à chaque Grand Prix 2024 |
| Clé primaire | `((driver_number, year), date_start, session_key)` |

Colonnes utilisées : `full_name` et `team_name` (pilote, écurie), `position` et `status` (arrivée : `FINISHED`, `DNF`, `DNS`, `DSQ`), `points`, `location` et `date_start` (Grand Prix).

## Environnement

```bash
# Spark + Jupyter
docker run -d --name spark-tp1 -p 8888:8888 -p 4040-4045:4040-4045 \
  -e JUPYTER_TOKEN=spark -v "$(pwd)":/home/jovyan/work quay.io/jupyter/pyspark-notebook

# Cluster Cassandra démarré (3 nœuds UN), puis Spark rejoint son réseau
docker exec -it cass1 nodetool status
docker network connect cass-net spark-tp1
```

Ouvrir ensuite [notebook/tp4_cassandra_spark.ipynb](notebook/tp4_cassandra_spark.ipynb) depuis http://localhost:8888/?token=spark. La Spark UI est sur http://localhost:4040.

## Traitements réalisés

Chaîne : **Cassandra → driver Python (`cassandra-driver`) → `spark.createDataFrame` → transformations → actions → Spark UI**

| Étape | Réalisation |
|---|---|
| Sélection | colonnes `full_name`, `team_name`, `location`, `date_start`, `position`, `status`, `points` |
| Filtres | podiums (`status = FINISHED` et `position <= 3`) : 72 ; non-classés (`status != FINISHED`) : 54 |
| Colonnes calculées | `pct_points_victoire` (points / 25 × 100) ; `categorie` (Podium / Points / Hors points / Non classé) ; `points_par_course`, `taux_podium_pct` |
| Agrégations | min / max / moyenne des points ; nombre de résultats par catégorie ; classement des écuries (`groupBy team_name`) ; championnat pilotes ; bilan par pilote |
| Partitions | 4 partitions initiales (`local[4]`), `repartition(8)` (avec shuffle), `coalesce(2)` (sans shuffle) |
| Lazy Evaluation | 4 transformations déclarées en ~0,01 s sans aucun job ; l'action `show()` déclenche 2 jobs |
| Shuffle | `Exchange hashpartitioning` (`groupBy`) et `Exchange rangepartitioning` (`orderBy`) dans `explain()` |

## Résultats

| Pilote | Écurie | Points | Podiums | Taux de podium | Position moy. |
|---|---|---|---|---|---|
| Max Verstappen | Red Bull Racing | 399 | 14 | 58,3 % | 3,0 |
| Lando Norris | McLaren | 344 | 13 | 54,2 % | 4,3 |
| Charles Leclerc | Ferrari | 327 | 13 | 54,2 % | 3,9 |
| Oscar Piastri | McLaren | 265 | 8 | 33,3 % | 5,1 |
| Carlos Sainz | Ferrari | 262 | 9 | 39,1 % | 4,1 |

Écuries : **McLaren 609 pts**, Ferrari 595 pts (22 podiums, le plus haut total), Red Bull 537 pts, Mercedes 433 pts.

- Verstappen domine individuellement.
- McLaren gagne chez les écuries grâce à deux pilotes réguliers. Red Bull repose surtout sur Verstappen : Pérez ne marque que 138 pts, avec 5 courses non terminées.
- Les totaux ne comptent que les Grands Prix. Les sprints ne sont pas dans la table, d'où l'écart avec le classement officiel.

## Observations Spark

**Pourquoi la Lazy Evaluation ?** Une transformation ne fait qu'enrichir le plan logique. Au moment de l'action, Catalyst optimise toute la chaîne d'un coup, par exemple :
- il ne lit que les colonnes utiles (`Project [full_name, points, team_name]`) ;
- il applique le filtre avant l'agrégation ;
- il fusionne les opérations d'un même stage.

On évite ainsi les calculs et les lectures inutiles.

**Spark UI** : pour `championnat.show(10)`, qui fait `filter`, `groupBy`, `sum`, `orderBy` :

- l'action lance **2 jobs** (29 et 30) ;
- **stage 40** : 4 tasks, une par partition, avec lecture, filtre et agrégation partielle. Shuffle Write : **1 784 B / 24 enregistrements** ;
- **stage 41** : *skipped*, car la sortie du shuffle est réutilisée ;
- **stage 42** : agrégation finale et top 10. Shuffle Read : **1 784 B / 24 enregistrements**. L'AQE a fusionné les 4 partitions de shuffle en **1 task** ;
- le graphe SQL montre le parcours des données : 479 lignes, puis 240 après le filtre, puis 24 après l'agrégation partielle, puis **Exchange** (shuffle), puis 22 pilotes, puis `TakeOrderedAndProject`. Avec `show(10)`, le tri global est remplacé par un top-N ;
- un seul executor (`driver`) avec 4 cœurs, ce qui correspond au mode `local[4]`.

## Captures

| Capture | Contenu |
|---|---|
| [01_dataframe](screenshots/01_dataframe.png) | DataFrame créé depuis Cassandra, schéma, nombre de lignes |
| [02_partitions](screenshots/02_partitions.png) | partitions initiales, `repartition(8)`, `coalesce(2)` |
| [03_lazy_evaluation](screenshots/03_lazy_evaluation.png) | transformations sans job, puis action |
| [04_plan_execution](screenshots/04_plan_execution.png) | `explain(True)` avec les `Exchange` |
| [05_analyse_metier](screenshots/05_analyse_metier.png) | bilan par pilote |
| [06_spark_ui_jobs](screenshots/06_spark_ui_jobs.png) | liste des Jobs |
| [07_spark_ui_job_championnat](screenshots/07_spark_ui_job_championnat.png) | détail d'un Job : stages exécutés / skipped |
| [08_spark_ui_stage_shuffle_write](screenshots/08_spark_ui_stage_shuffle_write.png) | stage 40 : 4 tasks, Shuffle Write |
| [09_spark_ui_stage_shuffle_read](screenshots/09_spark_ui_stage_shuffle_read.png) | stage 42 : 1 task, Shuffle Read |
| [10_spark_ui_executors](screenshots/10_spark_ui_executors.png) | executor `driver`, 4 cœurs |
| [11_spark_ui_sql_dag](screenshots/11_spark_ui_sql_dag.png) | plan SQL avec Exchange et AQEShuffleRead |
