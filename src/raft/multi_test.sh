#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage $0 numTrials(10, 20...) testPoint(2A, 2B, 2C, 2D)"
  exit 1
fi

rm trail_failed_*.log

passed=0
failed=0
RED='\033[31m'
GREEN='\033[32m'
RESET='\033[0m'

for i in $(seq 1 $1); do
  echo -e "${GREEN}Running trail $i out of all $1 trails${RESET}" 
  go test -run $2 > tmp_raft_file.log &
  GO_TEST_PID=$!
  tail -f tmp_raft_file.log &
  TAIL_PID=$!

  wait $GO_TEST_PID
  GO_EXIT_CODE=$?

  kill $TAIL_PID
  wait $TAIL_PID 2> /dev/null

  if [ $GO_EXIT_CODE -eq 0 ]; then
    passed=$((passed+1))
    echo -e "${GREEN}Trail $i passed${RESET}"
    rm tmp_raft_file.log
  else
    failed=$((failed+1))
    echo -e "${RED}Trail $i failed${RESET}"
    mv tmp_raft_file.log "trail_failed_$i.log"
  fi
done

echo -e "${GREEN}All $1 trails completed${RESET}"
echo -e "${GREEN}Passd: $passed${RESET}"
echo -e "${RED}Failed: $failed${RESET}"

