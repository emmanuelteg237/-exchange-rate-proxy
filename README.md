

\# 💱 Exchange Rate Proxy — Application Temps Réel



> Pipeline temps réel \*\*API → Kafka → Elasticsearch → Kibana\*\* pour centraliser les appels de taux de change et réduire les coûts : un seul service collecte, toutes les équipes consomment depuis Kafka.



!\[Python](https://img.shields.io/badge/python-3.11-blue.svg)

!\[Kafka](https://img.shields.io/badge/Kafka-3.5+-orange.svg)

!\[Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11-yellow.svg)

!\[Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)



\---



\## 🎯 Contexte

Une entreprise dispose de \*\*15 000 équipes\*\* consommant des taux de change via une API externe.  

Si chaque équipe appelle l’API, le coût explose.  

Ce projet \*\*centralise\*\* la collecte des taux dans un unique service, puis \*\*diffuse\*\* en temps réel via Kafka.



\---



\## 🏗️ Architecture



```mermaid

graph TD

A\[API exchangerate-api] -->|HTTPS| B\[Producer Python]

B -->|publish| C\[Kafka topic: exchange-rates]

C -->|consume| D\[Consumer ES]

C -->|consume| E\[Consumer Alert]

D -->|index| F\[Elasticsearch index: exchange-rates]

E -->|publish| G\[Kafka topic: exchange-alerts]

E -->|index| H\[Elasticsearch index: exchange-alerts]

F -->|query| I\[Kibana Dashboard]

H -->|query| I



