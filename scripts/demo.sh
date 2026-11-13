#!/usr/bin/env bash
# 端到端演示脚本：导入 → 首次查询 → 逆序复用 → 不同 ε 独立扣费 → 预算 → 拒绝。
# 用法：./scripts/demo.sh [BASE_URL]，默认 http://localhost:8080（compose 的 nginx）。
set -euo pipefail

BASE="${1:-http://localhost:8080}"
CSV="$(dirname "$0")/../sample_rides.csv"

j() { python3 -m json.tool; }

echo "== 1. 健康检查（注意 X-Worker-Id 会在两个 API 进程间轮询） =="
curl -sS -D - "$BASE/health" -o /dev/null | grep -i "x-worker-id" || true
curl -sS "$BASE/health" | j

echo "== 2. 导入 CSV（重复导入会 409） =="
curl -sS -X POST "$BASE/import" -F "file=@$CSV;type=text/csv" | j

echo "== 3. 首次查询 ε=0.2（预留→实扣，reused=false） =="
curl -sS -X POST "$BASE/query" -H 'Content-Type: application/json' \
  -d '{"stations":["人民广场","陆家嘴"],"epsilon":0.2}' | j

echo "== 4. 逆序同集合同 ε：复用同一噪声，零扣费（reused=true, charged=0） =="
curl -sS -X POST "$BASE/query" -H 'Content-Type: application/json' \
  -d '{"stations":["陆家嘴","人民广场"],"epsilon":0.2}' | j

echo "== 5. 同集合换 ε=0.5：独立采样、独立扣费 =="
curl -sS -X POST "$BASE/query" -H 'Content-Type: application/json' \
  -d '{"stations":["人民广场","陆家嘴"],"epsilon":0.5}' | j

echo "== 6. 预算账本 =="
curl -sS "$BASE/budget" | j

echo "== 7. 任务状态 =="
curl -sS "$BASE/tasks" | j

echo "== 8. 连续大额查询直至预算耗尽，观察 402 拒绝且预算不泄漏 =="
for eps in 0.5 0.2 0.2 0.1 0.1; do
  code=$(curl -sS -o /tmp/dp_resp.json -w '%{http_code}' -X POST "$BASE/query" \
    -H 'Content-Type: application/json' \
    -d "{\"stations\":[\"站$RANDOM\"],\"epsilon\":$eps}")
  echo "ε=$eps -> HTTP $code $( [ "$code" != "200" ] && cat /tmp/dp_resp.json )"
done
curl -sS "$BASE/budget" | j
