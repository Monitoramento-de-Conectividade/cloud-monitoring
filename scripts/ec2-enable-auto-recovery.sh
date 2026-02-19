#!/usr/bin/env bash
set -euo pipefail

# Configura alarmes do CloudWatch para recuperar/reiniciar automaticamente uma EC2.
# Uso:
#   INSTANCE_ID=i-0123456789abcdef AWS_REGION=us-east-1 bash scripts/ec2-enable-auto-recovery.sh
# Ou:
#   bash scripts/ec2-enable-auto-recovery.sh i-0123456789abcdef us-east-1

if ! command -v aws >/dev/null 2>&1; then
  echo "Erro: AWS CLI nao encontrado. Instale e configure antes de continuar." >&2
  exit 1
fi

INSTANCE_ID="${INSTANCE_ID:-${1:-}}"
REGION="${AWS_REGION:-${2:-}}"
ALARM_PREFIX="${ALARM_PREFIX:-cloud-monitoring}"

if [ -z "${INSTANCE_ID}" ]; then
  echo "Erro: defina INSTANCE_ID (ex.: i-0123456789abcdef)." >&2
  echo "Exemplo: INSTANCE_ID=i-0123456789abcdef AWS_REGION=us-east-1 bash scripts/ec2-enable-auto-recovery.sh" >&2
  exit 1
fi

if [ -z "${REGION}" ]; then
  REGION="$(aws configure get region || true)"
fi

if [ -z "${REGION}" ]; then
  echo "Erro: regiao nao definida. Informe AWS_REGION (ex.: us-east-1)." >&2
  exit 1
fi

if ! aws ec2 describe-instances \
  --region "${REGION}" \
  --instance-ids "${INSTANCE_ID}" \
  --query "Reservations[0].Instances[0].InstanceId" \
  --output text >/dev/null 2>&1; then
  echo "Erro: nao foi possivel validar a instancia ${INSTANCE_ID} na regiao ${REGION}." >&2
  exit 1
fi

SYSTEM_ALARM_NAME="${ALARM_PREFIX}-${INSTANCE_ID}-system-recover"
INSTANCE_ALARM_NAME="${ALARM_PREFIX}-${INSTANCE_ID}-instance-reboot"

echo "Criando/atualizando alarme de auto-recovery (falha de sistema)..."
aws cloudwatch put-metric-alarm \
  --region "${REGION}" \
  --alarm-name "${SYSTEM_ALARM_NAME}" \
  --alarm-description "Auto recovery para ${INSTANCE_ID} em falha de system status check" \
  --namespace AWS/EC2 \
  --metric-name StatusCheckFailed_System \
  --dimensions "Name=InstanceId,Value=${INSTANCE_ID}" \
  --statistic Maximum \
  --period 60 \
  --evaluation-periods 2 \
  --datapoints-to-alarm 2 \
  --threshold 0 \
  --comparison-operator GreaterThanThreshold \
  --treat-missing-data missing \
  --alarm-actions "arn:aws:automate:${REGION}:ec2:recover"

echo "Criando/atualizando alarme de auto-reboot (falha de instancia)..."
aws cloudwatch put-metric-alarm \
  --region "${REGION}" \
  --alarm-name "${INSTANCE_ALARM_NAME}" \
  --alarm-description "Auto reboot para ${INSTANCE_ID} em falha de instance status check" \
  --namespace AWS/EC2 \
  --metric-name StatusCheckFailed_Instance \
  --dimensions "Name=InstanceId,Value=${INSTANCE_ID}" \
  --statistic Maximum \
  --period 60 \
  --evaluation-periods 2 \
  --datapoints-to-alarm 2 \
  --threshold 0 \
  --comparison-operator GreaterThanThreshold \
  --treat-missing-data missing \
  --alarm-actions "arn:aws:automate:${REGION}:ec2:reboot"

echo
echo "Alarmes aplicados com sucesso:"
echo "- ${SYSTEM_ALARM_NAME} (action: ec2:recover)"
echo "- ${INSTANCE_ALARM_NAME} (action: ec2:reboot)"
echo
echo "Para conferir no CLI:"
echo "aws cloudwatch describe-alarms --region ${REGION} --alarm-names ${SYSTEM_ALARM_NAME} ${INSTANCE_ALARM_NAME}"

