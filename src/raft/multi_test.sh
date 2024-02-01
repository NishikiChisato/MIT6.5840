#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage $0 numTrials(10, 20...) testPoint(2A, 2B, 2C, 2D)"
  exit 1
fi

passed=0
failed=0
RED='\033[31m'
GREEN='\033[32m'
RESET='\033[0m'

for i in $(seq 1 $1); do
  echo -e "${GREEN}Running trail $i out of all $1 trails${RESET}" 
  go test -run $2 | tee tmp_raft_file.log
  if [ $? -eq 0 ]; then
    passed=$((passed+1))
    echo -e "${GREEN}Trail $i passed${RESET}"
    rm tmp_raft_file.log
  else
    failed=$((failed+1))
    echo -e "${RED}Trail $i failed${RESET}"
    mv tmp_raft_file "trail_failed_$1.log"
  fi
done

echo -e "${GREEN}All $1 trails completed${RESET}"
echo -e "${GREEN}Passd: $passed${RESET}"
echo -e "${RED}Failed: $failed${RESET}"

