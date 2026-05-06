#!/usr/bin/env bash
set -euo pipefail

SESSION="streaming-metrics"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$DIR/.venv/bin/activate"
ACTIVATE="source '$VENV' && export TZ=UTC"

# Pulsar health check — polls the admin API
WAIT_PULSAR="echo 'Waiting for Pulsar...' && until curl -sf http://localhost:8080/admin/v2/clusters > /dev/null 2>&1; do sleep 2; done && echo 'Pulsar ready.'"

if ! command -v tmux &>/dev/null; then
  echo "tmux is not installed. Run: brew install tmux" >&2
  exit 1
fi

# Kill any existing session cleanly
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Layout:
#  ┌─────────────────┬─────────────────┐
#  │   Spark Job     │     Pulsar      │
#  ├─────────────────┼─────────────────┤
#  │   Simulator     │   Streamlit     │
#  └─────────────────┴─────────────────┘

tmux new-session  -d -s "$SESSION" -x 220 -y 50

# Split into top/bottom halves, then split each half left/right
tmux split-window -v  -t "$SESSION:0.0" -p 50
tmux split-window -h  -t "$SESSION:0.0"
tmux split-window -h  -t "$SESSION:0.2"

# Pane 0 — top-left: Spark job (waits for Pulsar)
tmux send-keys -t "$SESSION:0.0" \
  "cd '$DIR' && $ACTIVATE && $WAIT_PULSAR && python -m metrics.jobs" Enter

# Pane 1 — top-right: Pulsar
tmux send-keys -t "$SESSION:0.1" \
  "cd '$DIR' && docker compose up pulsar" Enter

# Pane 2 — bottom-left: Simulator (waits for Pulsar)
tmux send-keys -t "$SESSION:0.2" \
  "cd '$DIR' && $ACTIVATE && $WAIT_PULSAR && python -m simulator.simulate" Enter

# Pane 3 — bottom-right: Streamlit (waits for Pulsar)
tmux send-keys -t "$SESSION:0.3" \
  "cd '$DIR' && $ACTIVATE && $WAIT_PULSAR && streamlit run ui/app.py" Enter

tmux attach-session -t "$SESSION"
