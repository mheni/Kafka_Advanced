#!/bin/bash
# drp_test_volume.sh
# Variante du script DRP mesurant le RTO en fonction du volume de données
# accumulé pendant la panne région West, avec calcul du RPO réel.
#
# Usage : ./drp_test_volume.sh <topic> <bootstrap-servers>
# Exemple : ./drp_test_volume.sh vls-stations-nancy 10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096

TOPIC="${1:-vls-stations-nancy}"
BOOTSTRAP="${2:-10.18.0.5:9092,10.118.0.5:9094,10.128.0.5:9096}"
KAFKA_BIN="/opt/kafka/bin"
LOGFILE="/home/kafka/drp_test_volume_$(date +%Y%m%d_%H%M%S).log"
PRODUCER_LOG="/home/kafka/producer_during_outage_$(date +%Y%m%d_%H%M%S).log"

log() {
  echo "[$(date +%H:%M:%S)] $1" | tee -a "$LOGFILE"
}

get_urp_count() {
  "$KAFKA_BIN/kafka-topics.sh" --describe --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" 2>/dev/null \
    | awk -F'Replicas: |\tIsr: ' '{ if (NF>=3) { n=split($2,r,","); m=split($3,i,","); if (m<n) c++ } } END { print c+0 }'
}

get_offline_count() {
  "$KAFKA_BIN/kafka-topics.sh" --describe --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --unavailable-partitions 2>/dev/null \
    | grep -c "Leader: none"
}

get_partition_count() {
  "$KAFKA_BIN/kafka-topics.sh" --describe --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" 2>/dev/null \
    | grep -c "^	Topic:"
}

get_log_end_offsets_sum() {
  "$KAFKA_BIN/kafka-run-class.sh" kafka.tools.GetOffsetShell \
    --broker-list "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --time -1 2>/dev/null \
    | awk -F: '{sum+=$3} END {print sum+0}'
}

topic_exists() {
  "$KAFKA_BIN/kafka-topics.sh" --list --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -Fxq "$TOPIC"
}

log "=========================================="
log "DRP TEST VOLUME - Topic cible: $TOPIC"
log "=========================================="

log "Étape 0: Vérification de l'existence du topic"
if ! topic_exists; then
  log "Le topic '$TOPIC' n'existe pas sur ce cluster."
  read -r -p "Voulez-vous le créer maintenant avec 90 partitions / RF=3 ? (y/n) " CREATE_CHOICE
  if [[ "$CREATE_CHOICE" == "y" || "$CREATE_CHOICE" == "Y" ]]; then
    "$KAFKA_BIN/kafka-topics.sh" --create \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --partitions 90 \
      --replication-factor 3 \
      --config min.insync.replicas=2 | tee -a "$LOGFILE"
    log "Topic '$TOPIC' créé."
  else
    log "Arrêt du script : aucun topic valide à tester."
    exit 1
  fi
fi

log "Étape 1: Vérification santé initiale du cluster"
"$KAFKA_BIN/kafka-topics.sh" --describe --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" | tee -a "$LOGFILE"

PARTITION_COUNT=$(get_partition_count)
OFFSET_BEFORE=$(get_log_end_offsets_sum)
log "Nombre de partitions: $PARTITION_COUNT"
log "Somme des log-end-offsets AVANT panne: $OFFSET_BEFORE"

log "Étape 2: PRÊT à simuler la panne région West."
log ">>> Exécutez maintenant manuellement sur VM1 le script stop_west_region.sh <<<"
read -r -p "Appuyez sur ENTRÉE une fois la région West arrêtée pour démarrer la mesure..."

OUTAGE_START=$(date +%s)
log "Chronomètre démarré à $(date +%H:%M:%S) (début de panne)"

log "Étape 3: Génération de charge pendant la panne (accumulation de données à resynchroniser)"
log "Lancement d'un producer de charge en arrière-plan pendant l'indisponibilité..."

nohup "$KAFKA_BIN/kafka-producer-perf-test.sh" \
  --topic "$TOPIC" \
  --num-records 500000 \
  --record-size 200 \
  --throughput 500 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=all \
  > "$PRODUCER_LOG" 2>&1 &
PRODUCER_PID=$!
log "Producer de charge démarré (PID $PRODUCER_PID), log: $PRODUCER_LOG"

log "Étape 4: PRÊT à restaurer la région West."
log ">>> Exécutez maintenant manuellement sur VM1 le script restore_west_region.sh <<<"
read -r -p "Appuyez sur ENTRÉE une fois la région West redémarrée..."

OUTAGE_END=$(date +%s)
OUTAGE_DURATION=$((OUTAGE_END - OUTAGE_START))
log "Panne restaurée. Durée de la panne: ${OUTAGE_DURATION} secondes"

if kill -0 "$PRODUCER_PID" 2>/dev/null; then
  log "Arrêt du producer de charge (encore actif)"
  kill "$PRODUCER_PID" 2>/dev/null
  wait "$PRODUCER_PID" 2>/dev/null
fi

RECORDS_ACCUMULATED=$(get_log_end_offsets_sum)
VOLUME_DELTA=$((RECORDS_ACCUMULATED - OFFSET_BEFORE))

log "=========================================="
log "VOLUME DE DONNÉES ACCUMULÉ PENDANT LA PANNE"
log "=========================================="
log "Offset total AVANT panne : $OFFSET_BEFORE"
log "Offset total APRÈS restauration : $RECORDS_ACCUMULATED"
log "VOLUME_ACCUMULE=$VOLUME_DELTA messages"
log "DUREE_PANNE=${OUTAGE_DURATION}s"

log "Étape 5: Surveillance du rebalancing jusqu'à récupération complète"
log "Attente du retour à un état healthy (0 URP, 0 offline)..."

REBALANCE_START=$(date +%s)
CHECK=0

while true; do
  CHECK=$((CHECK+1))
  URP=$(get_urp_count)
  OFFLINE=$(get_offline_count)
  log "Check $CHECK - Under-replicated: $URP | Offline: $OFFLINE"

  if [[ "$URP" -eq 0 && "$OFFLINE" -eq 0 ]]; then
    break
  fi
  sleep 10
done

REBALANCE_END=$(date +%s)
RTO_SECONDS=$((REBALANCE_END - REBALANCE_START))
RTO_TOTAL_SECONDS=$((REBALANCE_END - OUTAGE_START))

FINAL_OFFSET=$(get_log_end_offsets_sum)
MESSAGES_LOST=$((VOLUME_DELTA - (FINAL_OFFSET - OFFSET_BEFORE)))
if [[ "$MESSAGES_LOST" -lt 0 ]]; then
  MESSAGES_LOST=0
fi

THROUGHPUT_RATIO="N/A"
if [[ "$RTO_SECONDS" -gt 0 && "$VOLUME_DELTA" -gt 0 ]]; then
  THROUGHPUT_RATIO=$(echo "scale=2; $VOLUME_DELTA / $RTO_SECONDS" | bc)
fi

log "=========================================="
log "CLUSTER HEALTHY - Rebalancing terminé"
log "=========================================="
log "RTO (rebalancing seul, après restauration) = ${RTO_SECONDS}s"
log "RTO (panne + rebalancing, bout-en-bout)     = ${RTO_TOTAL_SECONDS}s"
log "RPO (messages potentiellement non confirmés au moment de la coupure) = ${MESSAGES_LOST} message(s)"
log "VOLUME_ACCUMULE_PENDANT_PANNE = ${VOLUME_DELTA} message(s)"
log "DEBIT_RESYNCHRONISATION = ${THROUGHPUT_RATIO} message(s)/seconde"
log "=========================================="

echo "" | tee -a "$LOGFILE"
echo "===== RÉSUMÉ POUR RAPPORT =====" | tee -a "$LOGFILE"
echo "Topic                         : $TOPIC" | tee -a "$LOGFILE"
echo "Partitions                    : $PARTITION_COUNT" | tee -a "$LOGFILE"
echo "Durée de la panne              : ${OUTAGE_DURATION}s" | tee -a "$LOGFILE"
echo "Volume accumulé pendant panne  : ${VOLUME_DELTA} messages" | tee -a "$LOGFILE"
echo "RTO (rebalancing)              : ${RTO_SECONDS}s" | tee -a "$LOGFILE"
echo "RTO (bout-en-bout)             : ${RTO_TOTAL_SECONDS}s" | tee -a "$LOGFILE"
echo "RPO (perte potentielle)        : ${MESSAGES_LOST} messages" | tee -a "$LOGFILE"
echo "Débit de resynchronisation     : ${THROUGHPUT_RATIO} msg/s" | tee -a "$LOGFILE"
echo "================================" | tee -a "$LOGFILE"

log "Log complet disponible dans: $LOGFILE"
