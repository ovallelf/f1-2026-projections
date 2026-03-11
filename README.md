# F1 2026 Race Time Projections

A Python desktop app that projects total race times and championship standings for all 22 drivers across the 2026 Formula 1 calendar.

![Python](https://img.shields.io/badge/Python-3.12+-blue) ![License](https://img.shields.io/badge/License-MIT-green)

## How It Works

The model starts from **Australian GP FP2 lap times** as baselines, then scales each driver's time per circuit using:

- **Circuit ratios** — lap-time scaling relative to Albert Park
- **Team affinities** — per-team multipliers for power, technical, and mixed circuit types
- **Historical performance** — driver-specific circuit affinity from 2023–2025 results (via [f1nsight-api-2](https://github.com/praneeth7781/f1nsight-api-2))
- **Global calibration** — auto-corrects projections using actual race winner times as races complete

Each race projection includes:

| Metric | Description |
|--------|-------------|
| **Projected Race Time** | Baseline × circuit ratio × team affinity × historical factor × calibration |
| **DNF%** | Per-driver retirement probability blending team reliability + driver history |
| **E[Pts]** | Expected points accounting for DNF risk and circuit overtaking difficulty |
| **Range** | 80% confidence interval for finishing position |

The **season championship view** sums actual points for completed races and projected E[Pts] for remaining rounds.

## Data Sources

- **FP2 baselines** — 2026 Australian GP Free Practice 2 session times
- **Historical data** — [f1nsight-api-2](https://github.com/praneeth7781/f1nsight-api-2) (2023–2025 race/qualifying positions)
- **Latest results** — [f1db](https://github.com/f1db/f1db) (current-season standings, race results, qualifying)

## Screenshot

The app displays a dark-themed Tkinter GUI with:

- Per-circuit race projections with team-colored rows
- Driver and constructor championship standings
- Projected season championship table
- Auto-calibration status

## Getting Started

### Requirements

- Python 3.12+
- No external packages required (uses only `tkinter`, `sqlite3`, `urllib`, standard library)

### Run

```bash
python f1_projection_app.py
```

The app fetches historical and current-season data on startup (cached locally). An internet connection is needed for the first run.

## Project Structure

```text
f1_projection_app.py    # Single-file application (data + model + GUI)
```

## Model Details

### Circuit Ratios

Each circuit has a lap-time ratio relative to Albert Park (ratio = 1.000). For example, Spa has a ratio of 1.332 (longer laps) while Spielberg is 0.822 (shorter).

### Overtaking Difficulty

Ranges from 0.05 (Monaco, near impossible) to 0.85 (Monza, excellent). At low-overtaking circuits, position uncertainty is higher — E[Pts] uses Gaussian-weighted blending across neighboring positions.

### Calibration

After each completed race, the app computes `GLOBAL_CORRECTION = actual_winner_time / projected_winner_time` and applies it to all future projections. This corrects systematic bias in the FP2 baseline model.

## License

MIT
