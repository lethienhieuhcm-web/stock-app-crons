#!/bin/bash
# LOP DAO CHI MUC CO DONG: thu VCI IQ /shareholder dien rong,
# chi chay khung 19:00-24:00 va 05:00-08:30 (ky luat infra),
# moi luot 150 ma, resumable theo holder_collect_state.json.
# Gio Asia/Saigon = UTC+7, tinh tay tu UTC (khong phu thuoc tzdata:
# VM 2026-10-21 thieu /usr/share/zoneinfo/Asia/Saigon -> TZ=Asia/Saigon
# roi ve UTC, guard tuong 12:5x la 05:5x va chay luot ngoai khung).
UH=$(date -u +%H)
UM=$(date -u +%M)
HN=$(( (10#$UH * 60 + 10#$UM + 420) % 1440 ))
H=$(printf '%02d' $((HN / 60)))
M=$(printf '%02d' $((HN % 60)))
if ! { [ $HN -ge 1140 ] || { [ $HN -ge 300 ] && [ $HN -le 510 ]; }; }; then
  echo "ngoai khung gio thu thap ($H:$M) - bo qua"
  exit 0
fi
cd ~/workspace/stock-app
export http_proxy=http://hatch-egress-proxy:3128
export https_proxy=http://hatch-egress-proxy:3128
export no_proxy=localhost,127.0.0.1,::1
.venv5x/bin/python -u scripts/warehouse_holder_collect.py --limit 150
.venv5x/bin/python scripts/warehouse_holder_index.py stats
# PUBLISH CHI MUC DAO CO DONG (Chot 21:50 21/10): tab Co dong doc qua API.
.venv5x/bin/python -u scripts/publish_holders.py
# O-1/O-2 (Chot 19:05 23/10): chi tiet co dong theo ma cho
# analyze_holders + room ngoai theo ma + divergence CW/EW.
.venv5x/bin/python -u scripts/publish_holders_detail.py
.venv5x/bin/python -u scripts/publish_foreign_room.py
.venv5x/bin/python -u scripts/publish_divergence.py
