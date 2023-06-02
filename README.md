# Projet Big Data Velib

## Description

Ce projet consiste à collecter et traiter les données de disponibilité des vélos en libre-service à partir de l'API OpenData, et à les stocker dans un cluster Apache Kafka. Les données sont ensuite consommées en temps réel par une application Spark Structured Streaming, qui les agrège et calcule des statistiques sur la disponibilité des vélos.
(TODO) Les statistiques sont ensuite stockées dans un cluster Apache Cassandra pour une utilisation ultérieure.

## Prérequis

- Docker compose 2.X
- Python 3.X
- Java JDK 11
- pip 21.X ou supérieur

## Installation

1. Cloner le dépôt Git :

```
git clone https://github.com/zarveyy/projet_velib_big_data.git
```

2. Démarrer Kafka et Zookeeper :

```
cd projet_velib_big_data
docker-compose up -d
```

3. Installer les packages Python :

```
pip install -r requirements.txt
```

4. Démarrer l'application Spark Structured Streaming :

```
cd spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 streaming.py
```
```
# Si cela ne fonctionne pas, remplacez le 3.4.0 par votre version de spark, pour la connaitre
pyspark --version
```

5. Démarrer le consommateur Kafka :

```
cd kafka
python consumer.py
```

6. Démarrer le producteur Kafka :

```
cd kafka
python producer.py
```

## Utilisation

Une fois que l'ensemble du système est en cours d'exécution, les données de disponibilité des vélos en libre-service seront collectées et stockées dans le cluster Kafka ainsi que dans un fichier csv géneré. L'application Spark Structured Streaming les consommera et les traitera en temps réel pour calculer des statistiques sur la disponibilité des vélos. Les résultats seront stockés dans le cluster Cassandra et peuvent être utilisés pour prendre des décisions opérationnelles concernant la gestion des vélos en libre-service.