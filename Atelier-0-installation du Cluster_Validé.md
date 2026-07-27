# Atelier Guidé — Installation Kafka 4.2 KRaft (6 Brokers + 5 Controllers sur 3 VMs)

## Plan d'Adressage IP (réseau 192.168.104.0/24, VMnet8)

| VM | Hostname | Adresse IP | Rôle |
| :-- | :-- | :-- | :-- |
| VM1 | kafka-node1 | 10.18.0.5 | Broker 1, Broker 2, Controller 1, Controller 2 |
| VM2 | kafka-node2 | 10.118.0.5 | Broker 3, Broker 4, Controller 3, Controller 4 |
| VM3 | kafka-node3 | 10.128.0.5 | Broker 5, Broker 6, Controller 5 |

## Plan des Node IDs et Ports

| Node ID | Rôle | VM | Port Client (PLAINTEXT) | Port Controller | log.dirs |
| :-- | :-- | :-- | :-- | :-- | :-- |
| 1 | Broker | VM1 | 9092 | — | /data/kafka/broker1 |
| 2 | Broker | VM1 | 9093 | — | /data/kafka/broker2 |
| 3 | Broker | VM2 | 9094 | — | /data/kafka/broker3 |
| 4 | Broker | VM2 | 9095 | — | /data/kafka/broker4 |
| 5 | Broker | VM3 | 9096 | — | /data/kafka/broker5 |
| 6 | Broker | VM3 | 9097 | — | /data/kafka/broker6 |
| 101 | Controller | VM1 | — | 9192 | /data/kafka/controller1 |
| 102 | Controller | VM1 | — | 9193 | /data/kafka/controller2 |
| 103 | Controller | VM2 | — | 9194 | /data/kafka/controller3 |
| 104 | Controller | VM2 | — | 9195 | /data/kafka/controller4 |
| 105 | Controller | VM3 | — | 9196 | /data/kafka/controller5 |

> **Note :** Les node.id des controllers utilisent la plage 101-105 pour éviter tout conflit avec les node.id des brokers (1-6), conformément aux exigences KRaft qui imposent des ID uniques au sein du cluster.[^1]

***

## Étape 1 — Préparation Réseau VMware (à faire une fois, sur l'hôte)

1. Ouvrez **VMware Workstation → Edit → Virtual Network Editor**
2. Sélectionnez **VMnet8**, cochez "Use local DHCP service" **désactivé** si vous voulez des IP fixes, ou notez la plage DHCP pour éviter les conflits
3. Assignez les IP fixes suivantes à chaque VM (voir Étape 2)

***

## Étape 2 — Configuration Réseau sur Chaque VM Ubuntu 22.04

Sur **chaque VM**, éditez la configuration Netplan :

```bash
sudo hostnamectl set-hostname kafka-node1

sudo nano /etc/netplan/01-netcfg.yaml

```

**Sur VM1** (adapter `ens33` selon `ip a`) :

```yaml
network:
  version: 2
  ethernets:
    ens33:
      dhcp4: no
      addresses: [10.18.0.5/24]
      routes:
        - to: default
          via: 192.168.104.2
      nameservers:
        addresses: [8.8.8.8, 1.1.1.1]
```
```bash
sudo systemctl enable systemd-networkd
sudo netplan apply
ip addr show
```

Adapter l'adresse pour VM2 (`10.118.0.5/24`) et VM3 (`10.128.0.5/24`).

```bash
sudo chmod 600 /etc/netplan/00-installer-config.yaml
sudo chown root:root /etc/netplan/00-installer-config.yaml

sudo netplan apply
```

Sur chaque VM, éditez `/etc/hosts` pour ajouter les 3 nœuds :

```bash
sudo nano /etc/hosts
```

```
10.18.0.5  kafka-node1
10.118.0.5  kafka-node2
10.128.0.5  kafka-node3
```

Testez la connectivité entre VMs :

```bash
ping -c 3 kafka-node1
ping -c 3 kafka-node2
ping -c 3 kafka-node3
```


***

## Étape 3 — Prérequis Système (sur les 3 VMs)

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-21-jdk wget curl net-tools ufw openssl

java -version
```

Créez l'utilisateur dédié et les répertoires :

```bash
sudo useradd -m -s /bin/bash kafka
sudo passwd kafka 
sudo mkdir -p /opt/kafka /data/kafka
sudo chown -R kafka:kafka /opt/kafka /data/kafka
```

Configurez le firewall (UFW) pour ouvrir les ports nécessaires — **sur chaque VM** :

```bash
sudo ufw allow 22/tcp
sudo ufw allow 9092:9097/tcp
sudo ufw allow 9192:9196/tcp
sudo ufw allow ssh
sudo ufw --force enable
sudo ufw status
```

Augmentez les limites de fichiers ouverts (requis pour Kafka) :

```bash
sudo tee -a /etc/security/limits.conf <<EOF
kafka soft nofile 100000
kafka hard nofile 100000
EOF
```


***

## Étape 4 — Téléchargement et Installation de Kafka 4.2 (sur les 3 VMs)

```bash
su - kafka
cd /opt/kafka
wget https://downloads.apache.org/kafka/4.2.1/kafka_2.13-4.2.1.tgz
tar -xzf kafka_2.13-4.2.1.tgz
mv kafka_2.13-4.2.1/* .
rmdir kafka_2.13-4.2.1
rm kafka_2.13-4.2.1.tgz
```

> **Note version :** confirmez la version exacte 4.2.x disponible sur `https://downloads.apache.org/kafka/` au moment de l'installation, en respectant la préférence N-1 exprimée par le client.[^2]

***

## Étape 5 — Génération du Cluster ID (une seule fois, sur VM1)

```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```

Notez précieusement l'UUID généré (exemple : `eCFJfuyGTTG-G5wu7YCz4A`). **Ce même UUID sera utilisé sur les 3 VMs.**

***

## Étape 6 — Fichiers de Configuration des 6 Brokers

Créez le répertoire de configs sur chaque VM :

```bash
mkdir -p /opt/kafka/config/kraft-lab
```
Sur VM1, après l'Étape 6 et 7, vous devriez avoir :

```text
/opt/kafka/config/kraft-lab/
├── broker1.properties
├── broker2.properties
├── controller1.properties
└── controller2.properties
```
Sur VM2 :

```text
/opt/kafka/config/kraft-lab/
├── broker3.properties
├── broker4.properties
├── controller3.properties
└── controller4.properties
```
Sur VM3 :

```text
/opt/kafka/config/kraft-lab/
├── broker5.properties
├── broker6.properties
└── controller5.properties
```

### VM1 — `broker1.properties` (node.id=1)

```properties
process.roles=broker
node.id=1
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://10.18.0.5:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker1
broker.rack=West

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```


### VM1 — `broker2.properties` (node.id=2)

Identique, en changeant :

```properties
process.roles=broker
node.id=2
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9093
advertised.listeners=PLAINTEXT://10.18.0.5:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker2
broker.rack=West

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```


### VM2 — `broker3.properties` (node.id=3)

```properties
process.roles=broker
node.id=3
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9094
advertised.listeners=PLAINTEXT://10.118.0.5:9094
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker3
broker.rack=North

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```
### VM2 — `broker4.properties` (node.id=4)

```properties
process.roles=broker
node.id=4
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9095
advertised.listeners=PLAINTEXT://10.118.0.5:9095
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker4
broker.rack=North

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```


### VM3 — `broker5.properties` (node.id=5)

```properties
process.roles=broker
node.id=5
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9096
advertised.listeners=PLAINTEXT://10.128.0.5:9096
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker5
broker.rack=West

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```


### VM3 — `broker6.properties` (node.id=6)

```properties
process.roles=broker
node.id=6
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=PLAINTEXT://0.0.0.0:9097
advertised.listeners=PLAINTEXT://10.128.0.5:9097
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

log.dirs=/data/kafka/broker6
broker.rack=North

num.network.threads=8
num.io.threads=8
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
```


***

## Étape 7 — Fichiers de Configuration des 5 Controllers

### VM1 — `controller1.properties` (node.id=101)

```properties
process.roles=controller
node.id=101
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9192
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/data/kafka/controller1
```


### VM1 — `controller2.properties` (node.id=102)

```properties
process.roles=controller
node.id=102
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9193
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/data/kafka/controller2
```


### VM2 — `controller3.properties` (node.id=103)

```properties
process.roles=controller
node.id=103
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9194
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/data/kafka/controller3
```


### VM2 — `controller4.properties` (node.id=104)

```properties
process.roles=controller
node.id=104
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9195
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/data/kafka/controller4
```


### VM3 — `controller5.properties` (node.id=105)

```properties
process.roles=controller
node.id=105
controller.quorum.voters=101@10.18.0.5:9192,102@10.18.0.5:9193,103@10.118.0.5:9194,104@10.118.0.5:9195,105@10.128.0.5:9196

listeners=CONTROLLER://0.0.0.0:9196
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/data/kafka/controller5
```

> **Important :** avec 5 controllers, le quorum peut tolérer 2 pannes simultanées tout en restant opérationnel (formule 2N+1 = 5 pour N=2 pannes tolérées), conforme aux bonnes pratiques KRaft.[^1]

***

## Étape 8 — Formatage du Stockage (Kafka Storage Tool)

**Sur chaque VM**, pour chaque instance (broker et controller), exécutez le formatage avec le **même Cluster ID** généré à l'Étape 5 :
```bash
nano /home/kafka/format_vm1.sh
```


##  — Coller ce Contenu

```bash
#!/bin/bash
set -e

CLUSTER_ID="eCFJfuyGTTG-G5wu7YCz4A"   # remplacez par VOTRE UUID réel généré à l'Étape 5

echo "Formatage broker1..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker1.properties

echo "Formatage broker2..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker2.properties

echo "Formatage controller1..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/controller1.properties

echo "Formatage controller2..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/controller2.properties

echo "Formatage terminé sur VM1."
```

**Important :** remplacez `eCFJfuyGTTG-G5wu7YCz4A` par l'UUID que vous avez réellement obtenu à l'Étape 5.

## Étape 4 — Sauvegarder et Fermer nano

- `Ctrl + O` (sauvegarder)
- `Entrée` (confirmer le nom de fichier)
- `Ctrl + X` (quitter)


## — Rendre le Script Exécutable

```bash
chmod +x /home/kafka/format_vm1.sh
```


##  — Exécuter le Script

```bash
/home/kafka/format_vm1.sh
```


### Résultat Attendu

```
Formatage broker1...
Formatted /data/kafka/broker1 with metadata.version 4.2-IV0.
Formatage broker2...
Formatted /data/kafka/broker2 with metadata.version 4.2-IV0.
Formatage controller1...
Formatted /data/kafka/controller1 with metadata.version 4.2-IV0.
Formatage controller2...
Formatted /data/kafka/controller2 with metadata.version 4.2-IV0.
Formatage terminé sur VM1.
```


## Sur VM2 — Même Principe, Script Adapté

```bash
nano /home/kafka/format_vm2.sh
```

```bash
#!/bin/bash
set -e

CLUSTER_ID="eCFJfuyGTTG-G5wu7YCz4A"   # EXACTEMENT le même UUID que VM1

echo "Formatage broker3..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker3.properties

echo "Formatage broker4..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker4.properties

echo "Formatage controller3..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/controller3.properties

echo "Formatage controller4..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/controller4.properties

echo "Formatage terminé sur VM2."
```

```bash
chmod +x /home/kafka/format_vm2.sh
/home/kafka/format_vm2.sh
```


## Sur VM3 — Script Final

```bash
nano /home/kafka/format_vm3.sh
```

```bash
#!/bin/bash
set -e

CLUSTER_ID="eCFJfuyGTTG-G5wu7YCz4A"   # EXACTEMENT le même UUID

echo "Formatage broker5..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker5.properties

echo "Formatage broker6..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/broker6.properties

echo "Formatage controller5..."
/opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c /opt/kafka/config/kraft-lab/controller5.properties

echo "Formatage terminé sur VM3."
```

```bash
chmod +x /home/kafka/format_vm3.sh
/home/kafka/format_vm3.sh
```

***

## Étape 9 — Démarrage des Services

**Sur VM1**, créez le répertoire de logs avant de démarrer les services :

```bash
mkdir -p /opt/kafka/logs
```

✅ **Vérification :**

```bash
ls -ld /opt/kafka/logs
```


### Résultat Attendu

```
drwxr-xr-x 2 kafka kafka 4096 Jul 14 11:10 /opt/kafka/logs
```

Puis relancez les commandes de démarrage normalement :

```bash
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller1.properties > /opt/kafka/logs/controller1.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller2.properties > /opt/kafka/logs/controller2.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker1.properties > /opt/kafka/logs/broker1.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker2.properties > /opt/kafka/logs/broker2.log 2>&1 &
```


## Répéter sur VM2 et VM3

**Sur VM2** :

```bash
mkdir -p /opt/kafka/logs

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller3.properties > /opt/kafka/logs/controller3.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller4.properties > /opt/kafka/logs/controller4.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker3.properties > /opt/kafka/logs/broker3.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker4.properties > /opt/kafka/logs/broker4.log 2>&1 &
```

**Sur VM3** :

```bash
mkdir -p /opt/kafka/logs

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller5.properties > /opt/kafka/logs/controller5.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker5.properties > /opt/kafka/logs/broker5.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker6.properties > /opt/kafka/logs/broker6.log 2>&1 &
```
**commandes pour vérifications**

```bash
ps aux | grep kafka.Kafka | grep -v grep | grep -oP '(?<=kraft-lab/)[a-z0-9]+\.properties'
exemple si broker1 ne fonctionne pas
sudo lsof /data/kafka/broker1/.lock 2>/dev/null
et répeter 
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker1.properties > /opt/kafka/logs/broker1.log 2>&1 &

```
## Vérifier que les Services Ont Bien Démarré

```bash
tail -f /opt/kafka/logs/controller1.log
```


### Résultat Attendu

```
INFO [ControllerServer id=101] Finished starting controllers
```

Faites `Ctrl+C` pour sortir du `tail`, puis vérifiez le broker de la même façon :

```bash
tail -f /opt/kafka/logs/broker1.log
```


### Résultat Attendu

```
INFO [BrokerServer id=1] Kafka Server started
```


## Vérifier que les Process Tournent Bien en Arrière-Plan

```bash
ps aux | grep kafka.Kafka**Sur VM1**, créez le répertoire de logs avant de démarrer les services :

```bash
mkdir -p /opt/kafka/logs
```

✅ **Vérification :**

```bash
ls -ld /opt/kafka/logs
```


### Résultat Attendu

```
drwxr-xr-x 2 kafka kafka 4096 Jul 14 11:10 /opt/kafka/logs
```

Puis relancez les commandes de démarrage normalement :

```bash
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller1.properties > /opt/kafka/logs/controller1.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller2.properties > /opt/kafka/logs/controller2.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker1.properties > /opt/kafka/logs/broker1.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker2.properties > /opt/kafka/logs/broker2.log 2>&1 &
```


## Répéter sur VM2 et VM3

**Sur VM2** :

```bash
mkdir -p /opt/kafka/logs

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller3.properties > /opt/kafka/logs/controller3.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller4.properties > /opt/kafka/logs/controller4.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker3.properties > /opt/kafka/logs/broker3.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker4.properties > /opt/kafka/logs/broker4.log 2>&1 &
```

**Sur VM3** :

```bash
mkdir -p /opt/kafka/logs

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller5.properties > /opt/kafka/logs/controller5.log 2>&1 &

sleep 15

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker5.properties > /opt/kafka/logs/broker5.log 2>&1 &
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker6.properties > /opt/kafka/logs/broker6.log 2>&1 &
```


## Vérifier que les Services Ont Bien Démarré

```bash
tail -f /opt/kafka/logs/controller1.log
```


### Résultat Attendu

```
INFO [ControllerServer id=101] Finished starting controllers
```

Faites `Ctrl+C` pour sortir du `tail`, puis vérifiez le broker de la même façon :

```bash
tail -f /opt/kafka/logs/broker1.log
```


### Résultat Attendu

```
INFO [BrokerServer id=1] Kafka Server started
```


## Vérifier que les Process Tournent Bien en Arrière-Plan

```bash
ps aux | grep kafka.Kafka

***

## Étape 10 — Vérification du Cluster

Depuis n'importe quelle VM :

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.18.0.5:9192 \
  describe --status
```

Créez un topic de test avec réplication complète :

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  --topic test-drp-lab \
  --partitions 6 \
  --replication-factor 3
```

Vérifiez la répartition des replicas et leaders :

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server 10.18.0.5:9092 \
  --topic test-drp-lab
```


***

## Étape 11 — Automatisation avec Systemd (Recommandé pour la Formation)
# pour la création de services systemd donne moi les fichiers bash et comment les créer et executer étape par etape

Voici la procédure complète pour créer un service systemd pour chaque broker et controller, avec un script générique adapté à votre topologie (5 controllers + 6 brokers sur 3 VMs).[^1]

## Principe

Chaque VM n'héberge que ses propres process (VM1: brokers 1-2 + controllers 1-2; VM2: brokers 3-4 + controllers 3-4; VM3: brokers 5-6 + controller 5). Vous devez donc créer uniquement les fichiers `.service` correspondants sur chaque VM.[^1]

## Étape 1 — Créer le Template de Service (sur chaque VM)

Créez un fichier service générique pour les **brokers** :

```bash
sudo tee /etc/systemd/system/kafka-broker@.service > /dev/null <<'EOF'
[Unit]
Description=Apache Kafka Broker %i
Documentation=https://kafka.apache.org/documentation/
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/broker%i.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
```

Créez le template pour les **controllers** :

```bash
sudo tee /etc/systemd/system/kafka-controller@.service > /dev/null <<'EOF'
[Unit]
Description=Apache Kafka Controller %i
Documentation=https://kafka.apache.org/documentation/
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft-lab/controller%i.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
```

> **Note :** Le symbole `@` permet d'utiliser un seul template pour plusieurs instances (ex: `kafka-broker@1`, `kafka-broker@2`), en utilisant `%i` comme variable qui reprend l'ID passé après le `@`.

## Étape 2 — Recharger systemd

Sur chaque VM, après création des fichiers :

```bash
sudo systemctl daemon-reload
```


## Étape 3 — Activer et Démarrer les Services (par VM)

### Sur VM1 (kafka-node1) — brokers 1,2 + controllers 1,2

```bash
sudo systemctl enable kafka-controller@1
sudo systemctl enable kafka-controller@2
sudo systemctl enable kafka-broker@1
sudo systemctl enable kafka-broker@2

sudo systemctl start kafka-controller@1
sudo systemctl start kafka-controller@2
sudo systemctl start kafka-broker@1
sudo systemctl start kafka-broker@2
```


### Sur VM2 (kafka-node2) — brokers 3,4 + controllers 3,4

```bash
sudo systemctl enable kafka-controller@3
sudo systemctl enable kafka-controller@4
sudo systemctl enable kafka-broker@3
sudo systemctl enable kafka-broker@4

sudo systemctl start kafka-controller@3
sudo systemctl start kafka-controller@4
sudo systemctl start kafka-broker@3
sudo systemctl start kafka-broker@4
```


### Sur VM3 (kafka-node3) — brokers 5,6 + controller 5

```bash
sudo systemctl enable kafka-controller@5
sudo systemctl enable kafka-broker@5
sudo systemctl enable kafka-broker@6

sudo systemctl start kafka-controller@5
sudo systemctl start kafka-broker@5
sudo systemctl start kafka-broker@6
```

⚠️ **Important :** Avant de lancer via systemd, arrêtez proprement tous les process actuels lancés en `nohup`, sinon vous retomberez sur l'erreur de lock `.lock` déjà rencontrée :

```bash
ps aux | grep kafka.Kafka | grep -v grep
kill <PID des process nohup actuels>
```


## Étape 4 — Vérifier le Statut

Sur chaque VM, après démarrage :

```bash
sudo systemctl status kafka-broker@1
sudo systemctl status kafka-controller@1
```


### Résultat Attendu

```
● kafka-broker@1.service - Apache Kafka Broker 1
     Loaded: loaded (/etc/systemd/system/kafka-broker@.service; enabled)
     Active: active (running) since ...
```


## Étape 5 — Consulter les Logs

systemd redirige automatiquement les logs vers journald :

```bash
sudo journalctl -u kafka-broker@1 -f
sudo journalctl -u kafka-controller@1 -f
```


## Étape 6 — Valider le Redémarrage Automatique

Testez un redémarrage complet de VM1 pour vérifier que tout revient au boot :

```bash
sudo reboot
```

Après reconnexion :

```bash
sudo systemctl status kafka-broker@1 kafka-broker@2 kafka-controller@1 kafka-controller@2
```

Les 4 services doivent afficher `active (running)` sans intervention manuelle.

## Étape 7 — Revalider le Cluster Global

Une fois tous les services systemd actifs sur les 3 VMs :

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096 \
  describe --status
```

Répétez pour chaque instance (broker2, controller1, controller2, etc., sur chaque VM). Cela permet ensuite d'exécuter facilement les scénarios DRP du lab avec `systemctl stop kafka-broker3` pour simuler une panne région North.

***

## Checklist de Validation Avant le Jour J

- ☐ Ping croisé réussi entre les 3 VMs
- ☐ Les 5 controllers affichent un quorum sain (`describe --status`)
- ☐ Les 6 brokers apparaissent dans `kafka-metadata-quorum.sh`
- ☐ Un topic test avec RF=3 se réplique correctement sur les 3 racks
- ☐ Services systemd créés et testés (start/stop/restart) pour chaque instance
- ☐ Snapshot VMware pris après validation complète, pour restauration rapide entre sessions de lab

***

Cette configuration reproduit fidèlement la topologie de production (6 brokers, 5 controllers, répartition 2 régions via `broker.rack`) tout en tenant sur vos 3 VMs. Souhaitez-vous que je prépare maintenant le script du **scénario DRP** (arrêt simulé d'une région, mesure du temps de rebalancing, preferred leader election) basé sur cette infrastructure ?[^3]
<span style="display:none">[^10][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://kafka.apache.org/42/operations/kraft/

[^2]: Catch-up-formation-Apache-Kafka-Luxse-Transcript.txt

[^3]: Questionnaire-KAFKA-Answer.pdf

[^4]: https://www.reddit.com/r/apachekafka/comments/1iizee6/completely_confused_about_kraft_mode_setup_for/

[^5]: https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/2.2/html/using_amq_streams_on_rhel/assembly-kraft-mode-str

[^6]: https://oneuptime.com/blog/post/2026-01-21-kafka-kraft-no-zookeeper/view

[^7]: https://forum.confluent.io/t/best-practice-for-configuring-kafka-3-6-1-controllers-and-brokers/37514

[^8]: https://www.youtube.com/watch?v=OKxdK-YeEUA

[^9]: https://www.conduktor.io/glossary/understanding-kraft-mode-in-kafka

[^10]: https://www.linkedin.com/pulse/running-kafka-42-kraft-tiny-ec2-my-setup-notes-selim-reza-ko9uc

