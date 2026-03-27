"""F1 2026 Race Time Projection App

Projects total race times for all 22 drivers across the 2026 calendar
using Albert Park FP1/FP2/FP3 baselines (weighted composite) scaled by
circuit ratios, team-affinity multipliers, and race lap counts.  Includes
per-circuit overtaking difficulty coefficients and per-driver DNF
probabilities to compute expected points (E[Pts]).
"""

import tkinter as tk
from tkinter import ttk
import sqlite3
import urllib.request
import zipfile
import json
import os
import pathlib
import tempfile
import threading
import time
import math
import csv
import io

# ---------------------------------------------------------------------------
# Phase 1 – Data Layer
# ---------------------------------------------------------------------------

# Step 1.1 – Circuit catalogue (24 rounds)
# Laps = ceil(305 km / circuit length), except Monaco (~260 km tradition)
CIRCUITS_2026 = {
    "albert_park": {"name": "Australian GP", "location": "Melbourne", "km": 5.278, "turns": 14, "type": "mixed", "ratio": 1.000, "laps": 58},
    "shanghai": {"name": "Chinese GP", "location": "Shanghai", "km": 5.451, "turns": 16, "type": "mixed", "ratio": 1.156, "laps": 56},
    "suzuka": {"name": "Japanese GP", "location": "Suzuka", "km": 5.807, "turns": 18, "type": "technical", "ratio": 1.140, "laps": 53},
    "bahrain": {"name": "Bahrain GP", "location": "Sakhir", "km": 5.412, "turns": 15, "type": "mixed", "ratio": 1.146, "laps": 57},
    "jeddah": {"name": "Saudi Arabian GP", "location": "Jeddah", "km": 6.174, "turns": 27, "type": "power", "ratio": 1.094, "laps": 50},
    "miami": {"name": "Miami GP", "location": "Miami", "km": 5.412, "turns": 19, "type": "mixed", "ratio": 1.124, "laps": 57},
    "montreal": {"name": "Canadian GP", "location": "Montreal", "km": 4.361, "turns": 14, "type": "power", "ratio": 0.916, "laps": 70},
    "monaco": {"name": "Monaco GP", "location": "Monte Carlo", "km": 3.337, "turns": 19, "type": "technical", "ratio": 0.913, "laps": 78},
    "barcelona": {"name": "Spanish GP (Barcelona)", "location": "Barcelona", "km": 4.657, "turns": 14, "type": "technical", "ratio": 0.956, "laps": 66},
    "spielberg": {"name": "Austrian GP", "location": "Spielberg", "km": 4.318, "turns": 10, "type": "power", "ratio": 0.822, "laps": 71},
    "silverstone": {"name": "British GP", "location": "Silverstone", "km": 5.891, "turns": 18, "type": "power", "ratio": 1.091, "laps": 52},
    "spa": {"name": "Belgian GP", "location": "Spa-Francorchamps", "km": 7.004, "turns": 20, "type": "power", "ratio": 1.332, "laps": 44},
    "hungary": {"name": "Hungarian GP", "location": "Budapest", "km": 4.381, "turns": 14, "type": "technical", "ratio": 0.960, "laps": 70},
    "zandvoort": {"name": "Dutch GP", "location": "Zandvoort", "km": 4.259, "turns": 14, "type": "technical", "ratio": 0.891, "laps": 72},
    "monza": {"name": "Italian GP", "location": "Monza", "km": 5.793, "turns": 11, "type": "power", "ratio": 1.015, "laps": 53},
    "madrid": {"name": "Spanish GP (Madrid)", "location": "Madrid", "km": 5.474, "turns": 22, "type": "mixed", "ratio": 1.027, "laps": 56},
    "baku": {"name": "Azerbaijan GP", "location": "Baku", "km": 6.003, "turns": 20, "type": "mixed", "ratio": 1.291, "laps": 51},
    "singapore": {"name": "Singapore GP", "location": "Singapore", "km": 4.940, "turns": 19, "type": "technical", "ratio": 1.201, "laps": 62},
    "cota": {"name": "United States GP", "location": "Austin", "km": 5.513, "turns": 20, "type": "mixed", "ratio": 1.205, "laps": 56},
    "mexico": {"name": "Mexico City GP", "location": "Mexico City", "km": 4.304, "turns": 17, "type": "technical", "ratio": 0.974, "laps": 71},
    "interlagos": {"name": "São Paulo GP", "location": "São Paulo", "km": 4.309, "turns": 15, "type": "power", "ratio": 0.884, "laps": 71},
    "las_vegas": {"name": "Las Vegas GP", "location": "Las Vegas", "km": 6.201, "turns": 17, "type": "power", "ratio": 1.196, "laps": 50},
    "lusail": {"name": "Qatar GP", "location": "Lusail", "km": 5.419, "turns": 16, "type": "mixed", "ratio": 1.056, "laps": 57},
    "yas_marina": {"name": "Abu Dhabi GP", "location": "Abu Dhabi", "km": 5.281, "turns": 16, "type": "mixed", "ratio": 1.079, "laps": 58},
}

# Step 1.2 – Driver roster (22 drivers, FP1/FP2/FP3 baselines from Albert Park)
# data_quality: "measured" = representative lap set, "estimated" = indirect estimation
# fp1/fp2/fp3: best lap time in seconds from each session (None = no representative time)
DRIVERS_2026 = [
    {"name": "Oscar Piastri", "num": 81, "team": "McLaren", "tag": "mclaren", "fp1": 81.342, "fp2": 79.729, "fp3": 80.087, "data_quality": "measured"},
    {"name": "Kimi Antonelli", "num": 12, "team": "Mercedes", "tag": "mercedes", "fp1": 81.376, "fp2": 79.943, "fp3": 80.324, "data_quality": "measured"},
    {"name": "George Russell", "num": 63, "team": "Mercedes", "tag": "mercedes", "fp1": 81.371, "fp2": 80.049, "fp3": 79.053, "data_quality": "measured"},
    {"name": "Lewis Hamilton", "num": 44, "team": "Ferrari", "tag": "ferrari", "fp1": 80.736, "fp2": 80.050, "fp3": 79.669, "data_quality": "measured"},
    {"name": "Charles Leclerc", "num": 16, "team": "Ferrari", "tag": "ferrari", "fp1": 80.267, "fp2": 80.291, "fp3": 79.827, "data_quality": "measured"},
    {"name": "Max Verstappen", "num": 3, "team": "Red Bull Racing", "tag": "red_bull", "fp1": 80.789, "fp2": 80.366, "fp3": 80.197, "data_quality": "measured"},
    {"name": "Lando Norris", "num": 1, "team": "McLaren", "tag": "mclaren", "fp1": 84.391, "fp2": 80.794, "fp3": 80.443, "data_quality": "measured"},
    {"name": "Arvid Lindblad", "num": 41, "team": "Racing Bulls", "tag": "racing_bulls", "fp1": 81.313, "fp2": 80.922, "fp3": 80.838, "data_quality": "measured"},
    {"name": "Isack Hadjar", "num": 6, "team": "Red Bull Racing", "tag": "red_bull", "fp1": 81.087, "fp2": 80.941, "fp3": 80.137, "data_quality": "measured"},
    {"name": "Esteban Ocon", "num": 31, "team": "Haas", "tag": "haas", "fp1": 82.161, "fp2": 81.179, "fp3": 80.983, "data_quality": "measured"},
    {"name": "Oliver Bearman", "num": 87, "team": "Haas", "tag": "haas", "fp1": 82.682, "fp2": 81.326, "fp3": 80.778, "data_quality": "measured"},
    {"name": "Nico Hulkenberg", "num": 27, "team": "Audi", "tag": "audi", "fp1": 81.969, "fp2": 81.351, "fp3": 81.067, "data_quality": "measured"},
    {"name": "Liam Lawson", "num": 30, "team": "Racing Bulls", "tag": "racing_bulls", "fp1": 82.613, "fp2": 81.358, "fp3": 80.890, "data_quality": "measured"},
    {"name": "Gabriel Bortoleto", "num": 5, "team": "Audi", "tag": "audi", "fp1": 81.696, "fp2": 81.668, "fp3": 80.459, "data_quality": "measured"},
    {"name": "Alexander Albon", "num": 23, "team": "Williams", "tag": "williams", "fp1": 83.130, "fp2": 81.847, "fp3": 81.664, "data_quality": "measured"},
    {"name": "Pierre Gasly", "num": 10, "team": "Alpine", "tag": "alpine", "fp1": 84.035, "fp2": 82.167, "fp3": 81.071, "data_quality": "measured"},
    {"name": "Carlos Sainz", "num": 55, "team": "Williams", "tag": "williams", "fp1": 82.323, "fp2": 82.253, "fp3": None, "data_quality": "measured"},
    {"name": "Franco Colapinto", "num": 43, "team": "Alpine", "tag": "alpine", "fp1": 83.325, "fp2": 82.619, "fp3": 81.413, "data_quality": "measured"},
    {"name": "Valtteri Bottas", "num": 77, "team": "Cadillac", "tag": "cadillac", "fp1": 84.022, "fp2": 83.660, "fp3": 83.514, "data_quality": "measured"},
    {"name": "Fernando Alonso", "num": 14, "team": "Aston Martin", "tag": "aston_martin", "fp1": None, "fp2": None, "fp3": 82.720, "data_quality": "estimated"},
    {"name": "Lance Stroll", "num": 18, "team": "Aston Martin", "tag": "aston_martin", "fp1": None, "fp2": None, "fp3": None, "data_quality": "estimated"},
    {"name": "Sergio Perez", "num": 11, "team": "Cadillac", "tag": "cadillac", "fp1": 84.620, "fp2": None, "fp3": 84.397, "data_quality": "measured"},
]

# Step 1.2b – Session weights for composite baseline (later sessions more representative)
# FP3 is closest to qualifying/race conditions; FP1 is exploratory
FP_WEIGHTS = {"fp1": 0.20, "fp2": 0.35, "fp3": 0.45}

# Grid-midfield reference lap time at Albert Park (seconds). Used only to derive
# circuit reference times via ratio for circuits with no live FP data.
REFERENCE_LAP_TIME = 81.0


def compute_composite_baseline(driver: dict) -> float:
    """Compute a weighted composite baseline from available FP sessions.

    Uses FP_WEIGHTS, skipping sessions with no time and redistributing
    their weight proportionally. Returns the best single session time
    if only one is available.
    """
    sessions = {}
    for key in ("fp1", "fp2", "fp3"):
        t = driver.get(key)
        if t is not None:
            sessions[key] = t

    if not sessions:
        # Fallback for drivers with no FP data — use estimated value
        return 82.0  # grid-midfield estimate

    if len(sessions) == 1:
        return next(iter(sessions.values()))

    # Redistribute weights across available sessions
    total_weight = sum(FP_WEIGHTS[k] for k in sessions)
    return sum(sessions[k] * FP_WEIGHTS[k] / total_weight for k in sessions)

# Step 1.3 – Team affinities by circuit type
TEAM_AFFINITIES = {
    "mclaren":       {"power": 1.00, "technical": 0.99, "mixed": 1.00},
    "mercedes":      {"power": 0.99, "technical": 1.00, "mixed": 1.00},
    "ferrari":       {"power": 1.01, "technical": 1.00, "mixed": 1.00},
    "red_bull":      {"power": 1.00, "technical": 1.01, "mixed": 1.00},
    "racing_bulls":  {"power": 1.00, "technical": 1.00, "mixed": 1.00},
    "haas":          {"power": 0.99, "technical": 1.00, "mixed": 1.00},
    "audi":          {"power": 1.00, "technical": 1.01, "mixed": 1.00},
    "williams":      {"power": 1.01, "technical": 1.00, "mixed": 1.00},
    "alpine":        {"power": 1.00, "technical": 1.00, "mixed": 1.00},
    "cadillac":      {"power": 1.00, "technical": 1.00, "mixed": 1.00},
    "aston_martin":  {"power": 1.00, "technical": 1.00, "mixed": 1.00},
}

# Step 1.4 – Per-circuit overtaking difficulty (0.0 = near impossible, 1.0 = very easy)
# Based on historical overtaking statistics and circuit characteristics (2020-2025)
CIRCUIT_OVERTAKING = {
    "albert_park": 0.55,   # Moderate – improved after 2022 redesign
    "shanghai":    0.75,   # Good – long back straight + multiple DRS zones
    "suzuka":      0.35,   # Difficult – few overtaking zones, high-speed corners
    "bahrain":     0.80,   # Excellent – multiple DRS zones, wide braking zones
    "jeddah":      0.60,   # Moderate – fast street circuit, DRS effective
    "miami":       0.50,   # Moderate – limited overtaking zones
    "montreal":    0.65,   # Good – long straights, heavy braking chicanes
    "monaco":      0.05,   # Near impossible – narrowest track on calendar
    "barcelona":   0.40,   # Difficult – dirty air through sector 3
    "spielberg":   0.70,   # Good – short lap, multiple heavy braking zones
    "silverstone": 0.50,   # Moderate – high-speed, limited hard braking
    "spa":         0.65,   # Good – Kemmel straight, Les Combes hairpin
    "hungary":     0.20,   # Very difficult – narrow, twisty, "Monaco without walls"
    "zandvoort":   0.25,   # Very difficult – narrow, banked corners
    "monza":       0.85,   # Excellent – slipstream paradise, heavy braking
    "madrid":      0.55,   # Moderate – new circuit, estimated from layout
    "baku":        0.60,   # Moderate – long main straight, tight middle sector
    "singapore":   0.30,   # Difficult – narrow street circuit
    "cota":        0.55,   # Moderate – turn 1 braking, back straight DRS
    "mexico":      0.50,   # Moderate – long front straight, thin air braking
    "interlagos":  0.70,   # Good – short lap, Senna S and main straight
    "las_vegas":   0.65,   # Good – long straights, heavy braking
    "lusail":      0.45,   # Below average – flowing medium-speed circuit
    "yas_marina":  0.55,   # Moderate – improved after 2021 redesign
}

# Step 1.5 – Per-team DNF probability per race
# Estimated from PU reliability history and team operational track record (2023-2025)
# Lower = more reliable
TEAM_DNF_PROBABILITY = {
    "mclaren":       0.05,  # Mercedes PU, strong reliability record
    "mercedes":      0.05,  # Factory team, mature PU
    "ferrari":       0.07,  # Occasional PU/strategy-related retirements
    "red_bull":      0.06,  # Ford/RB PU, new partnership adds uncertainty
    "racing_bulls":  0.06,  # Same Ford/RB PU as Red Bull
    "haas":          0.08,  # Ferrari PU, smaller team resources
    "audi":          0.10,  # New PU manufacturer, highest mechanical risk
    "williams":      0.07,  # Mercedes PU, midfield team
    "alpine":        0.08,  # Renault PU, historically mixed reliability
    "cadillac":      0.12,  # New entrant, highest operational uncertainty
    "aston_martin":  0.07,  # Mercedes PU, solid infrastructure
}

# Step 1.6 – Team colours for GUI badges
TEAM_COLORS = {
    "mclaren":       {"bg": "#FF8000", "fg": "#000000"},
    "mercedes":      {"bg": "#00D2BE", "fg": "#000000"},
    "ferrari":       {"bg": "#DC0000", "fg": "#FFFFFF"},
    "red_bull":      {"bg": "#1E3A5F", "fg": "#FFFFFF"},
    "racing_bulls":  {"bg": "#2B4562", "fg": "#FFFFFF"},
    "haas":          {"bg": "#B6BABD", "fg": "#000000"},
    "audi":          {"bg": "#52E252", "fg": "#000000"},
    "williams":      {"bg": "#005AFF", "fg": "#FFFFFF"},
    "alpine":        {"bg": "#0090FF", "fg": "#FFFFFF"},
    "cadillac":      {"bg": "#1E1E1E", "fg": "#DAA520"},
    "aston_martin":  {"bg": "#006F62", "fg": "#FFFFFF"},
}

# ---------------------------------------------------------------------------
# Calibration Constants (update after each race weekend)
# ---------------------------------------------------------------------------
# GLOBAL_CORRECTION: ratio of actual winner race time to projected winner race time.
# Set to 1.0 (neutral) until Round 1 race data is available.
# After the race: GLOBAL_CORRECTION = actual_winner_time_s / projected_winner_time_s
GLOBAL_CORRECTION = 1.0
_GC_LOCK = threading.Lock()  # Protects GLOBAL_CORRECTION read/write (MAJ-01)

# OFFSET_FP_TO_QUALI: median seconds faster in qualifying vs composite FP across the grid.
# Set to 0.0 (neutral) until qualifying data is available.
# After qualifying: compute per-driver (composite_fp - quali_best), take median.
OFFSET_FP_TO_QUALI = 0.0

RACE_POINTS = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
SPRINT_POINTS = {1: 8, 2: 7, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}

SPRINT_CIRCUITS = {
    "shanghai":    {"sprint_laps": 19},
    "miami":       {"sprint_laps": 19},
    "montreal":    {"sprint_laps": 23},
    "silverstone": {"sprint_laps": 17},
    "zandvoort":   {"sprint_laps": 24},
    "singapore":   {"sprint_laps": 21},
}

SQ_TO_SPRINT_FACTOR = 1.03   # Sprint qualifying → sprint race pace (~3% degradation)
SPRINT_DNF_FACTOR = 0.50     # Sprint DNF probability = 50% of race DNF

SESSION_TYPE_MAP = {
    "FP1": "FREE_PRACTICE_1_RESULT",
    "FP2": "FREE_PRACTICE_2_RESULT",
    "FP3": "FREE_PRACTICE_3_RESULT",
    "SQ":  "SPRINT_QUALIFYING_RESULT",
    "Sprint": "SPRINT_RACE_RESULT",
    "Q":   "QUALIFYING_RESULT",
    "Race": "RACE_RESULT",
}

# ---------------------------------------------------------------------------
# F1DB Integration Constants (latest / current-season data)
# ---------------------------------------------------------------------------
F1DB_RELEASE_TAG = "v2026.0.1"
F1DB_SQLITE_URL = f"https://github.com/f1db/f1db/releases/download/{F1DB_RELEASE_TAG}/f1db-sqlite.zip"
F1DB_CACHE_DIR = pathlib.Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir())) / "f1-projection" / "f1db"
F1DB_DB_PATH = F1DB_CACHE_DIR / "f1db.db"
F1DB_VERSION_PATH = F1DB_CACHE_DIR / "version.json"

# ---------------------------------------------------------------------------
# f1nsight-api-2 Constants (historical data)
# ---------------------------------------------------------------------------
F1NSIGHT_BASE_URL = "https://raw.githubusercontent.com/praneeth7781/f1nsight-api-2/master"
F1NSIGHT_CACHE_DIR = pathlib.Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir())) / "f1-projection" / "f1nsight"
F1NSIGHT_CACHE_TTL_HOURS = 24  # Re-fetch driver files after this many hours

# ---------------------------------------------------------------------------
# f1db Raw GitHub Constants (fallback when SQLite release is not yet updated)
# ---------------------------------------------------------------------------
F1DB_RAW_BASE_URL = "https://raw.githubusercontent.com/f1db/f1db/main/src/data/seasons"

# Circuit key → race directory slug on GitHub
RACE_DIR_SLUGS = {
    "albert_park": "01-australia",
    "shanghai": "02-china",
    "suzuka": "03-japan",
    "bahrain": "04-bahrain",
    "jeddah": "05-saudi-arabia",
    "miami": "06-miami",
    "montreal": "07-canada",
    "monaco": "08-monaco",
    "barcelona": "09-barcelona-catalunya",
    "spielberg": "10-austria",
    "silverstone": "11-great-britain",
    "spa": "12-belgium",
    "hungary": "13-hungary",
    "zandvoort": "14-netherlands",
    "monza": "15-italy",
    "madrid": "16-spain",
    "baku": "17-azerbaijan",
    "singapore": "18-singapore",
    "cota": "19-united-states",
    "mexico": "20-mexico",
    "interlagos": "21-sao-paulo",
    "las_vegas": "22-las-vegas",
    "lusail": "23-qatar",
    "yas_marina": "24-abu-dhabi",
}


def download_f1db(progress_callback=None):
    """Download and extract f1db SQLite database from GitHub releases."""
    F1DB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = F1DB_CACHE_DIR / "f1db-sqlite.zip"

    if progress_callback:
        progress_callback("Downloading f1db database...")

    # Use urlopen with timeout instead of urlretrieve to avoid hanging indefinitely
    with urllib.request.urlopen(F1DB_SQLITE_URL, timeout=60) as resp, open(zip_path, "wb") as out:
        out.write(resp.read())

    if progress_callback:
        progress_callback("Extracting database...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the .db file inside the zip (may be nested)
        db_names = [n for n in zf.namelist() if n.endswith(".db")]
        if not db_names:
            raise FileNotFoundError("No .db file found in f1db-sqlite.zip")
        zf.extract(db_names[0], F1DB_CACHE_DIR)
        # Move to expected path if nested
        extracted = F1DB_CACHE_DIR / db_names[0]
        if extracted != F1DB_DB_PATH:
            extracted.rename(F1DB_DB_PATH)

    # Clean up zip
    zip_path.unlink(missing_ok=True)

    # Save version info
    version_info = {"tag": F1DB_RELEASE_TAG, "downloaded": str(pathlib.Path(__file__).stat().st_mtime)}
    F1DB_VERSION_PATH.write_text(json.dumps(version_info))

    if progress_callback:
        progress_callback("Database ready.")


def get_db_connection():
    """Get a read-only connection to the f1db database. Downloads if needed."""
    # Check if we need to download
    need_download = not F1DB_DB_PATH.exists()
    if not need_download and F1DB_VERSION_PATH.exists():
        try:
            cached_version = json.loads(F1DB_VERSION_PATH.read_text())
            if cached_version.get("tag") != F1DB_RELEASE_TAG:
                need_download = True
        except (json.JSONDecodeError, KeyError):
            need_download = True

    if need_download:
        try:
            download_f1db()
        except Exception:
            if not F1DB_DB_PATH.exists():
                return None  # Offline and no cache — graceful fallback

    try:
        conn = sqlite3.connect(f"file:{F1DB_DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


# ---------------------------------------------------------------------------
# f1nsight-api-2 ID Mappings (historical data)
# ---------------------------------------------------------------------------

# App display name → f1nsight driverId (Ergast-style slug)
DRIVER_F1NSIGHT_IDS = {
    "Oscar Piastri": "piastri",
    "Kimi Antonelli": "antonelli",
    "George Russell": "russell",
    "Lewis Hamilton": "hamilton",
    "Charles Leclerc": "leclerc",
    "Max Verstappen": "max_verstappen",
    "Lando Norris": "norris",
    "Arvid Lindblad": "lindblad",
    "Isack Hadjar": "hadjar",
    "Esteban Ocon": "ocon",
    "Oliver Bearman": "bearman",
    "Nico Hulkenberg": "hulkenberg",
    "Liam Lawson": "lawson",
    "Gabriel Bortoleto": "bortoleto",
    "Alexander Albon": "albon",
    "Pierre Gasly": "gasly",
    "Carlos Sainz": "sainz",
    "Franco Colapinto": "colapinto",
    "Valtteri Bottas": "bottas",
    "Fernando Alonso": "alonso",
    "Lance Stroll": "stroll",
    "Sergio Perez": "perez",
}

# GP name (as used by f1nsight/Ergast) → app circuit key
GP_NAME_TO_CIRCUIT_KEY = {
    "Australian Grand Prix": "albert_park",
    "Chinese Grand Prix": "shanghai",
    "Japanese Grand Prix": "suzuka",
    "Bahrain Grand Prix": "bahrain",
    "Saudi Arabian Grand Prix": "jeddah",
    "Miami Grand Prix": "miami",
    "Canadian Grand Prix": "montreal",
    "Monaco Grand Prix": "monaco",
    "Spanish Grand Prix": "barcelona",
    "Austrian Grand Prix": "spielberg",
    "British Grand Prix": "silverstone",
    "Belgian Grand Prix": "spa",
    "Hungarian Grand Prix": "hungary",
    "Dutch Grand Prix": "zandvoort",
    "Italian Grand Prix": "monza",
    "Azerbaijan Grand Prix": "baku",
    "Singapore Grand Prix": "singapore",
    "United States Grand Prix": "cota",
    "Mexico City Grand Prix": "mexico",
    "São Paulo Grand Prix": "interlagos",
    "Las Vegas Grand Prix": "las_vegas",
    "Qatar Grand Prix": "lusail",
    "Abu Dhabi Grand Prix": "yas_marina",
}

# ---------------------------------------------------------------------------
# F1DB ID Mappings (latest / current-season data)
# ---------------------------------------------------------------------------

DRIVER_F1DB_IDS = {
    "Oscar Piastri": "oscar-piastri",
    "Kimi Antonelli": "kimi-antonelli",
    "George Russell": "george-russell",
    "Lewis Hamilton": "lewis-hamilton",
    "Charles Leclerc": "charles-leclerc",
    "Max Verstappen": "max-verstappen",
    "Lando Norris": "lando-norris",
    "Arvid Lindblad": "arvid-lindblad",
    "Isack Hadjar": "isack-hadjar",
    "Esteban Ocon": "esteban-ocon",
    "Oliver Bearman": "oliver-bearman",
    "Nico Hulkenberg": "nico-hulkenberg",
    "Liam Lawson": "liam-lawson",
    "Gabriel Bortoleto": "gabriel-bortoleto",
    "Alexander Albon": "alexander-albon",
    "Pierre Gasly": "pierre-gasly",
    "Carlos Sainz": "carlos-sainz-jr",
    "Franco Colapinto": "franco-colapinto",
    "Valtteri Bottas": "valtteri-bottas",
    "Fernando Alonso": "fernando-alonso",
    "Lance Stroll": "lance-stroll",
    "Sergio Perez": "sergio-perez",
}

CIRCUIT_F1DB_IDS = {
    "albert_park": "melbourne",
    "shanghai": "shanghai",
    "suzuka": "suzuka",
    "bahrain": "bahrain",
    "jeddah": "jeddah",
    "miami": "miami",
    "montreal": "montreal",
    "monaco": "monaco",
    "barcelona": "catalunya",
    "spielberg": "spielberg",
    "silverstone": "silverstone",
    "spa": "spa-francorchamps",
    "hungary": "hungaroring",
    "zandvoort": "zandvoort",
    "monza": "monza",
    "madrid": "madrid",
    "baku": "baku",
    "singapore": "marina-bay",
    "cota": "austin",
    "mexico": "mexico-city",
    "interlagos": "interlagos",
    "las_vegas": "las-vegas",
    "lusail": "lusail",
    "yas_marina": "yas-marina",
}

# Reverse lookups — built once at module level (MAJ-02)
_F1DB_TO_DRIVER = {v: k for k, v in DRIVER_F1DB_IDS.items()}
_F1DB_TO_CIRCUIT = {v: k for k, v in CIRCUIT_F1DB_IDS.items()}

# formula1-datasets CSV cross-reference source
F1_DATASETS_BASE_URL = "https://raw.githubusercontent.com/toUpperCase78/formula1-datasets/master"
F1_DATASETS_FILES = {
    "race_results": "Formula1_{year}Season_RaceResults.csv",
    "qualifying": "Formula1_{year}Season_QualifyingResults.csv",
    "sprint_results": "Formula1_{year}Season_SprintResults.csv",
    "sprint_qualifying": "Formula1_{year}Season_SprintQualifyingResults.csv",
}
CSV_TRACK_TO_CIRCUIT_KEY = {
    "Australia": "albert_park",
    "China": "shanghai",
    "Japan": "suzuka",
    "Bahrain": "bahrain",
    "Saudi Arabia": "jeddah",
    "Miami": "miami",
    "Canada": "montreal",
    "Monaco": "monaco",
    "Spain": "barcelona",
    "Madrid": "madrid",
    "Austria": "spielberg",
    "Great Britain": "silverstone",
    "Belgium": "spa",
    "Hungary": "hungary",
    "Netherlands": "zandvoort",
    "Italy": "monza",
    "Azerbaijan": "baku",
    "Singapore": "singapore",
    "United States": "cota",
    "Mexico": "mexico",
    "Brazil": "interlagos",
    "Las Vegas": "las_vegas",
    "Qatar": "lusail",
    "Abu Dhabi": "yas_marina",
}

CONSTRUCTOR_F1DB_IDS = {
    "mclaren": ["mclaren"],
    "mercedes": ["mercedes"],
    "ferrari": ["ferrari"],
    "red_bull": ["red-bull"],
    "racing_bulls": ["racing-bulls", "alphatauri", "toro-rosso"],
    "haas": ["haas"],
    "audi": ["audi", "kick-sauber", "alfa-romeo", "sauber"],
    "williams": ["williams"],
    "alpine": ["alpine", "renault"],
    "cadillac": ["cadillac"],
    "aston_martin": ["aston-martin", "racing-point", "force-india"],
}

# ---------------------------------------------------------------------------
# Driver Categories & Historical Weights
# ---------------------------------------------------------------------------

DRIVER_CATEGORIES = {
    # Experienced, same team (alpha 0.30-0.35)
    "Oscar Piastri": {"category": "experienced_same", "alpha": 0.35},
    "George Russell": {"category": "experienced_same", "alpha": 0.35},
    "Charles Leclerc": {"category": "experienced_same", "alpha": 0.35},
    "Lando Norris": {"category": "experienced_same", "alpha": 0.35},
    "Alexander Albon": {"category": "experienced_same", "alpha": 0.30},
    "Pierre Gasly": {"category": "experienced_same", "alpha": 0.30},
    "Fernando Alonso": {"category": "experienced_same", "alpha": 0.35},
    "Lance Stroll": {"category": "experienced_same", "alpha": 0.30},
    # Experienced, new team (alpha 0.15-0.20)
    "Lewis Hamilton": {"category": "experienced_new", "alpha": 0.20},
    "Max Verstappen": {"category": "experienced_new", "alpha": 0.20},
    "Esteban Ocon": {"category": "experienced_new", "alpha": 0.20},
    "Nico Hulkenberg": {"category": "experienced_new", "alpha": 0.20},
    "Carlos Sainz": {"category": "experienced_new", "alpha": 0.20},
    "Liam Lawson": {"category": "experienced_new", "alpha": 0.15},
    # 2025 rookies (alpha 0.10)
    "Kimi Antonelli": {"category": "rookie_2025", "alpha": 0.10},
    "Oliver Bearman": {"category": "rookie_2025", "alpha": 0.10},
    "Gabriel Bortoleto": {"category": "rookie_2025", "alpha": 0.10},
    "Isack Hadjar": {"category": "rookie_2025", "alpha": 0.10},
    # Gap-year veterans (alpha 0.10)
    "Valtteri Bottas": {"category": "gap_veteran", "alpha": 0.10},
    "Sergio Perez": {"category": "gap_veteran", "alpha": 0.10},
    # True rookie — no F1 history (alpha 0.00)
    "Arvid Lindblad": {"category": "true_rookie", "alpha": 0.00},
    # Partial F1 (alpha 0.05)
    "Franco Colapinto": {"category": "partial_f1", "alpha": 0.05},
}


# ---------------------------------------------------------------------------
# f1nsight-api-2 Data Functions (historical data)
# ---------------------------------------------------------------------------

def _fetch_f1nsight_driver_json(driver_id: str) -> dict | None:
    """Fetch a single driver's JSON from f1nsight-api-2, with local caching.

    Caches to F1NSIGHT_CACHE_DIR/{driver_id}.json. Returns parsed dict or None.
    """
    F1NSIGHT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = F1NSIGHT_CACHE_DIR / f"{driver_id}.json"

    # Check if cached file is fresh enough
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < F1NSIGHT_CACHE_TTL_HOURS:
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass  # stale/corrupt — re-fetch

    # Download from GitHub raw URL
    url = f"{F1NSIGHT_BASE_URL}/drivers/{driver_id}.json"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            raw = resp.read()
        data = json.loads(raw)
        cache_path.write_bytes(raw)
        return data
    except Exception:
        # Fall back to stale cache if available
        if cache_path.exists():
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return None


def build_historical_data(progress_callback=None) -> dict:
    """Fetch historical data from f1nsight-api-2 for all 2026 drivers.

    Fetches per-driver JSON files from the static GitHub API and extracts
    race finishing positions for 2023-2025 at 2026-calendar circuits.

    Returns nested dict:
        {driver_name: {circuit_key: {"avg_finish": float, "races": int,
         "wins": int, "podiums": int, "dnfs": int, "points": float}}}
    """
    historical = {}
    total = len(DRIVER_F1NSIGHT_IDS)

    for idx, (driver_name, f1nsight_id) in enumerate(DRIVER_F1NSIGHT_IDS.items(), 1):
        if progress_callback:
            progress_callback(f"Loading {driver_name} ({idx}/{total})...")

        data = _fetch_f1nsight_driver_json(f1nsight_id)
        if data is None:
            continue

        driver_circuits = {}

        # Extract race positions for 2023-2025
        race_pos = data.get("racePosition", {})
        # Also extract DNFs
        dnf_data = data.get("DNFs", {})
        # And podiums
        podium_data = data.get("podiums", {})

        for year_str in ["2023", "2024", "2025"]:
            year_block = race_pos.get(year_str, {})
            positions = year_block.get("positions", {})

            year_dnfs = dnf_data.get(year_str, {})
            year_podiums = podium_data.get(year_str, {})

            for gp_name, pos in positions.items():
                circuit_key = GP_NAME_TO_CIRCUIT_KEY.get(gp_name)
                if circuit_key is None:
                    continue  # GP not on 2026 calendar

                if circuit_key not in driver_circuits:
                    driver_circuits[circuit_key] = {
                        "finishes": [],
                        "wins": 0,
                        "podiums": 0,
                        "dnfs": 0,
                        "points": 0.0,
                        "quali_positions": [],
                    }

                entry = driver_circuits[circuit_key]

                try:
                    pos_int = int(pos)
                except (ValueError, TypeError):
                    # Non-numeric position = DNF/DSQ/DNS
                    entry["dnfs"] += 1
                    continue

                entry["finishes"].append(pos_int)
                if pos_int == 1:
                    entry["wins"] += 1
                if pos_int <= 3:
                    entry["podiums"] += 1
                entry["points"] += float(RACE_POINTS.get(pos_int, 0))

            # Count additional DNFs from DNFs data not already captured
            for gp_name in year_dnfs:
                circuit_key = GP_NAME_TO_CIRCUIT_KEY.get(gp_name)
                if circuit_key is None:
                    continue
                if circuit_key not in driver_circuits:
                    driver_circuits[circuit_key] = {
                        "finishes": [],
                        "wins": 0,
                        "podiums": 0,
                        "dnfs": 0,
                        "points": 0.0,
                        "quali_positions": [],
                    }
                # DNFs are already counted if racePosition has a non-numeric value
                # But if racePosition didn't include this race, count it here
                if gp_name not in positions:
                    driver_circuits[circuit_key]["dnfs"] += 1

        # Extract qualifying positions for 2023-2025
        quali_pos = data.get("qualiPosition", {})
        for year_str in ["2023", "2024", "2025"]:
            year_block = quali_pos.get(year_str, {})
            positions = year_block.get("positions", {})
            for gp_name, pos in positions.items():
                circuit_key_q = GP_NAME_TO_CIRCUIT_KEY.get(gp_name)
                if circuit_key_q is None:
                    continue
                if circuit_key_q not in driver_circuits:
                    driver_circuits[circuit_key_q] = {
                        "finishes": [], "wins": 0, "podiums": 0, "dnfs": 0,
                        "points": 0.0, "quali_positions": [],
                    }
                entry = driver_circuits[circuit_key_q]
                if "quali_positions" not in entry:
                    entry["quali_positions"] = []
                try:
                    entry["quali_positions"].append(int(pos))
                except (ValueError, TypeError):
                    pass

        # Convert to final format with avg_finish
        if driver_circuits:
            historical[driver_name] = {}
            for circuit_key, cdata in driver_circuits.items():
                finishes = cdata["finishes"]
                races = len(finishes) + cdata["dnfs"]
                avg_finish = (sum(finishes) / len(finishes)) if finishes else 11.0
                quali = cdata.get("quali_positions", [])
                historical[driver_name][circuit_key] = {
                    "avg_finish": avg_finish,
                    "avg_quali": (sum(quali) / len(quali)) if quali else None,
                    "races": races,
                    "wins": cdata["wins"],
                    "podiums": cdata["podiums"],
                    "dnfs": cdata["dnfs"],
                    "points": cdata["points"],
                }

    return historical


# ---------------------------------------------------------------------------
# F1DB Historical Data (legacy — kept for reference / future current-season use)
# ---------------------------------------------------------------------------

def build_historical_data_f1db(conn):
    """Query f1db SQLite for 2023-2025 race results. Legacy — replaced by f1nsight."""
    if conn is None:
        return {}

    historical = {}

    try:
        rows = conn.execute(
            """
            SELECT rd.driver_id, rd.position_number, rd.race_points,
                   rd.race_reason_retired, r.circuit_id, r.year
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'RACE_RESULT' AND r.year BETWEEN 2023 AND 2025
            ORDER BY r.year DESC
            """
        ).fetchall()

        for row in rows:
            driver_name = _F1DB_TO_DRIVER.get(row["driver_id"])
            circuit_key = _F1DB_TO_CIRCUIT.get(row["circuit_id"])
            if not driver_name or not circuit_key:
                continue
            if driver_name not in historical:
                historical[driver_name] = {}
            if circuit_key not in historical[driver_name]:
                historical[driver_name][circuit_key] = {"finishes": [], "wins": 0, "podiums": 0, "dnfs": 0, "points": 0.0}
            entry = historical[driver_name][circuit_key]
            pos = row["position_number"]
            if pos is None or row["race_reason_retired"]:
                entry["dnfs"] += 1
            else:
                entry["finishes"].append(pos)
                if pos == 1: entry["wins"] += 1
                if pos <= 3: entry["podiums"] += 1
            entry["points"] += float(row["race_points"] or 0)
    except sqlite3.Error:
        return {}

    result = {}
    for dn, circuits in historical.items():
        result[dn] = {}
        for ck, d in circuits.items():
            f = d["finishes"]
            result[dn][ck] = {"avg_finish": (sum(f)/len(f)) if f else 11.0, "races": len(f)+d["dnfs"], **{k: d[k] for k in ("wins","podiums","dnfs","points")}}
    return result


# ---------------------------------------------------------------------------
# f1db Latest Data Queries (current-season standings, results, calendar)
# ---------------------------------------------------------------------------

def fetch_current_standings(conn, year=2026):
    """Query f1db for current driver and constructor championship standings."""
    if conn is None:
        return [], []
    try:
        latest_race = conn.execute("""
            SELECT MAX(r.id) FROM race r
            JOIN race_data rd ON rd.race_id = r.id
            WHERE rd.type = 'RACE_RESULT' AND r.year = ?
        """, (year,)).fetchone()[0]
        if latest_race is None:
            return [], []
        driver_rows = conn.execute("""
            SELECT rds.position_number, rds.driver_id, rds.points
            FROM race_driver_standing rds
            WHERE rds.race_id = ?
            ORDER BY rds.position_display_order
        """, (latest_race,)).fetchall()
        driver_standings = []
        for row in driver_rows:
            name = _F1DB_TO_DRIVER.get(row["driver_id"], row["driver_id"])
            driver_standings.append({
                "position": row["position_number"],
                "driver": name,
                "driver_id": row["driver_id"],
                "points": float(row["points"]),
            })
        constructor_rows = conn.execute("""
            SELECT rcs.position_number, rcs.constructor_id, rcs.points
            FROM race_constructor_standing rcs
            WHERE rcs.race_id = ?
            ORDER BY rcs.position_display_order
        """, (latest_race,)).fetchall()
        constructor_standings = []
        for row in constructor_rows:
            constructor_standings.append({
                "position": row["position_number"],
                "constructor_id": row["constructor_id"],
                "points": float(row["points"]),
            })
        return driver_standings, constructor_standings
    except sqlite3.Error:
        return [], []


def fetch_latest_race_result(conn, year=2026):
    """Query f1db for the most recent completed race result."""
    if conn is None:
        return None
    try:
        race_info = conn.execute("""
            SELECT r.id, r.round, r.date, r.grand_prix_id, r.circuit_id
            FROM race r
            JOIN race_data rd ON rd.race_id = r.id
            WHERE rd.type = 'RACE_RESULT' AND r.year = ?
            ORDER BY r.round DESC LIMIT 1
        """, (year,)).fetchone()
        if race_info is None:
            return None
        race_id = race_info["id"]
        rows = conn.execute("""
            SELECT rd.position_number, rd.driver_id, rd.constructor_id,
                   rd.race_time, rd.race_points, rd.race_fastest_lap
            FROM race_data rd
            WHERE rd.race_id = ? AND rd.type = 'RACE_RESULT'
            ORDER BY rd.position_display_order
        """, (race_id,)).fetchall()
        results = []
        for row in rows:
            name = _F1DB_TO_DRIVER.get(row["driver_id"], row["driver_id"])
            results.append({
                "position": row["position_number"],
                "driver": name,
                "driver_id": row["driver_id"],
                "constructor_id": row["constructor_id"],
                "time": row["race_time"],
                "points": float(row["race_points"] or 0),
                "fastest_lap": bool(row["race_fastest_lap"]),
            })
        return {
            "round": race_info["round"],
            "grand_prix": race_info["grand_prix_id"],
            "circuit_id": race_info["circuit_id"],
            "date": race_info["date"],
            "results": results,
        }
    except sqlite3.Error:
        return None


def fetch_season_calendar(conn, year=2026):
    """Query f1db for the race calendar with completion status."""
    if conn is None:
        return []
    try:
        rows = conn.execute("""
            SELECT r.id, r.round, r.date, r.grand_prix_id, r.circuit_id,
                   r.sprint_qualifying_format IS NOT NULL AS is_sprint,
                   COUNT(rd.race_id) > 0 AS completed
            FROM race r
            LEFT JOIN race_data rd ON rd.race_id = r.id AND rd.type = 'RACE_RESULT'
            WHERE r.year = ?
            GROUP BY r.id
            ORDER BY r.round
        """, (year,)).fetchall()
        calendar = []
        for row in rows:
            calendar.append({
                "round": row["round"],
                "date": row["date"],
                "grand_prix": row["grand_prix_id"],
                "circuit_id": row["circuit_id"],
                "is_sprint": bool(row["is_sprint"]),
                "completed": bool(row["completed"]),
            })
        return calendar
    except sqlite3.Error:
        return []


def fetch_session_completion(conn, year=2026):
    """Query f1db for completed session types per circuit."""
    if conn is None:
        return {}
    try:
        rows = conn.execute("""
            SELECT r.circuit_id, rd.type
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE r.year = ? AND rd.type IN (
                'FREE_PRACTICE_1_RESULT', 'FREE_PRACTICE_2_RESULT',
                'FREE_PRACTICE_3_RESULT', 'QUALIFYING_RESULT',
                'SPRINT_QUALIFYING_RESULT', 'SPRINT_RACE_RESULT',
                'RACE_RESULT'
            )
            GROUP BY r.circuit_id, rd.type
        """, (year,)).fetchall()
        result = {}
        for row in rows:
            circuit_key = _F1DB_TO_CIRCUIT.get(row["circuit_id"])
            if circuit_key:
                result.setdefault(circuit_key, set()).add(row["type"])
        return result
    except sqlite3.Error:
        return {}


def fetch_qualifying_results(conn, circuit_key, year=2026):
    """Query f1db for qualifying results at the given circuit in the current season.

    Returns dict mapping driver_name -> qualifying_position (int), or empty dict.
    """
    if conn is None:
        return {}
    f1db_circuit = CIRCUIT_F1DB_IDS.get(circuit_key)
    if f1db_circuit is None:
        return {}
    try:
        rows = conn.execute("""
            SELECT rd.driver_id, rd.position_number
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'QUALIFYING_RESULT'
              AND r.year = ? AND r.circuit_id = ?
            ORDER BY rd.position_display_order
        """, (year, f1db_circuit)).fetchall()
        results = {}
        for row in rows:
            driver_name = _F1DB_TO_DRIVER.get(row["driver_id"])
            if driver_name:
                results[driver_name] = row["position_number"]
        return results
    except sqlite3.Error:
        return {}


def fetch_qualifying_times(conn, circuit_key, year=2026):
    """Query f1db for qualifying lap times at the given circuit.

    Returns dict mapping driver_name -> best qualifying time in seconds, or empty dict.
    """
    if conn is None:
        return {}
    f1db_circuit = CIRCUIT_F1DB_IDS.get(circuit_key)
    if f1db_circuit is None:
        return {}
    try:
        rows = conn.execute("""
            SELECT rd.driver_id, rd.q1, rd.q2, rd.q3
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'QUALIFYING_RESULT'
              AND r.year = ? AND r.circuit_id = ?
        """, (year, f1db_circuit)).fetchall()
        results = {}
        for row in rows:
            driver_name = _F1DB_TO_DRIVER.get(row["driver_id"])
            if not driver_name:
                continue
            best_t = None
            for col in ("q3", "q2", "q1"):
                val = row[col] if col in row.keys() else None
                if val is not None:
                    t = _parse_quali_time(str(val))
                    if t is not None:
                        best_t = t
                        break
            if best_t is not None:
                results[driver_name] = best_t
        return results
    except (sqlite3.Error, sqlite3.OperationalError):
        return {}


def fetch_sprint_qualifying_results(conn, circuit_key, year=2026):
    """Fetch sprint qualifying grid positions from f1db SQLite."""
    if conn is None:
        return {}
    f1db_circuit = CIRCUIT_F1DB_IDS.get(circuit_key)
    if f1db_circuit is None:
        return {}
    try:
        rows = conn.execute("""
            SELECT rd.driver_id, rd.position_number
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'SPRINT_QUALIFYING_RESULT'
              AND r.year = ? AND r.circuit_id = ?
            ORDER BY rd.position_display_order
        """, (year, f1db_circuit)).fetchall()
        results = {}
        for row in rows:
            driver_name = _F1DB_TO_DRIVER.get(row["driver_id"])
            if driver_name:
                results[driver_name] = row["position_number"]
        return results
    except sqlite3.Error:
        return {}


def fetch_sprint_qualifying_times(conn, circuit_key, year=2026):
    """Fetch best sprint qualifying lap times from f1db SQLite.

    Note: f1db reuses q1/q2/q3 columns for sprint qualifying data,
    differentiated by type='SPRINT_QUALIFYING_RESULT' in race_data.
    """
    if conn is None:
        return {}
    f1db_circuit = CIRCUIT_F1DB_IDS.get(circuit_key)
    if f1db_circuit is None:
        return {}
    try:
        rows = conn.execute("""
            SELECT rd.driver_id, rd.q1, rd.q2, rd.q3
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'SPRINT_QUALIFYING_RESULT'
              AND r.year = ? AND r.circuit_id = ?
        """, (year, f1db_circuit)).fetchall()
        results = {}
        for row in rows:
            driver_name = _F1DB_TO_DRIVER.get(row["driver_id"])
            if not driver_name:
                continue
            best_t = None
            for col in ("q3", "q2", "q1"):
                val = row[col] if col in row.keys() else None
                if val is not None:
                    t = _parse_quali_time(str(val))
                    if t is not None:
                        best_t = t
                        break
            if best_t is not None:
                results[driver_name] = best_t
        return results
    except sqlite3.Error:
        return {}


def compute_auto_calibration(conn, year=2026):
    """Compute GLOBAL_CORRECTION from actual vs projected winner times."""
    if conn is None:
        return 1.0, 0
    try:
        rows = conn.execute("""
            SELECT r.circuit_id, rd.race_time_millis
            FROM race_data rd
            JOIN race r ON rd.race_id = r.id
            WHERE rd.type = 'RACE_RESULT' AND r.year = ?
              AND rd.position_number = 1 AND rd.race_time_millis IS NOT NULL
            ORDER BY r.round
        """, (year,)).fetchall()
        if not rows:
            return 1.0, 0
        # Save and reset GLOBAL_CORRECTION to avoid recursive distortion (V-02)
        global GLOBAL_CORRECTION
        with _GC_LOCK:
            saved_gc = GLOBAL_CORRECTION
            GLOBAL_CORRECTION = 1.0
        ratios = []
        for row in rows:
            circuit_key = _F1DB_TO_CIRCUIT.get(row["circuit_id"])
            if circuit_key is None:
                continue
            actual_seconds = row["race_time_millis"] / 1000.0
            projections = calculate_all_projections(circuit_key)
            if not projections:
                continue
            projected_seconds = projections[0]["time_s"]  # V-01 fix: key is time_s
            if projected_seconds > 0:
                ratios.append(actual_seconds / projected_seconds)
        # Restore GLOBAL_CORRECTION before returning
        with _GC_LOCK:
            GLOBAL_CORRECTION = saved_gc
        if not ratios:
            return 1.0, 0
        avg_ratio = sum(ratios) / len(ratios)
        return round(avg_ratio, 6), len(ratios)
    except (sqlite3.Error, KeyError):
        return 1.0, 0


# ---------------------------------------------------------------------------
# f1db Raw GitHub Data Functions (fallback data source)
# ---------------------------------------------------------------------------

def _yaml_val(s: str):
    """Convert a YAML scalar string to a Python type."""
    if not s or s.lower() in ('null', '~'):
        return None
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    if s.lower() == 'true':
        return True
    if s.lower() == 'false':
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def _parse_simple_yaml_list(text: str) -> list[dict]:
    """Parse a simple YAML list-of-dicts (as used by f1db source files)."""
    items = []
    current = None
    for line in text.splitlines():
        stripped = line.rstrip()
        if not stripped or stripped.startswith('#'):
            continue
        if stripped.startswith('- '):
            if current is not None:
                items.append(current)
            current = {}
            kv = stripped[2:].strip()
            if ':' in kv:
                key, _, val = kv.partition(':')
                current[key.strip()] = _yaml_val(val.strip())
        elif current is not None and ':' in stripped:
            key, _, val = stripped.strip().partition(':')
            current[key.strip()] = _yaml_val(val.strip())
    if current is not None:
        items.append(current)
    return items


def _fetch_raw_text(url: str) -> str | None:
    """Fetch raw text from a URL. Returns None on any failure."""
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.read().decode('utf-8')
    except Exception:
        return None


def fetch_csv_qualifying(year=2026, progress_callback=None):
    """Fetch qualifying results from formula1-datasets CSV."""
    filename = F1_DATASETS_FILES["qualifying"].format(year=year)
    url = f"{F1_DATASETS_BASE_URL}/{filename}"
    if progress_callback:
        progress_callback("Cross-referencing qualifying data (CSV)...")
    text = _fetch_raw_text(url)
    if text is None:
        return {}, {}
    driver_names = {d["name"] for d in DRIVERS_2026}
    quali_positions = {}
    quali_times = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        track = row.get("Track", "").strip()
        circuit_key = CSV_TRACK_TO_CIRCUIT_KEY.get(track)
        if not circuit_key:
            continue
        driver = row.get("Driver", "").strip()
        if driver not in driver_names:
            continue
        try:
            pos = int(row.get("Position", "0"))
        except (ValueError, TypeError):
            continue
        quali_positions.setdefault(circuit_key, {})[driver] = pos
        for qkey in ("Q3", "Q2", "Q1"):
            t = _parse_quali_time(row.get(qkey, ""))
            if t is not None:
                quali_times.setdefault(circuit_key, {})[driver] = t
                break
    return quali_positions, quali_times


def fetch_csv_sprint_qualifying(year=2026, progress_callback=None):
    """Fetch sprint qualifying results from formula1-datasets CSV."""
    filename = F1_DATASETS_FILES["sprint_qualifying"].format(year=year)
    url = f"{F1_DATASETS_BASE_URL}/{filename}"
    if progress_callback:
        progress_callback("Cross-referencing sprint qualifying data (CSV)...")
    text = _fetch_raw_text(url)
    if text is None:
        return {}, {}
    driver_names = {d["name"] for d in DRIVERS_2026}
    sq_positions = {}
    sq_times = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        track = row.get("Track", "").strip()
        circuit_key = CSV_TRACK_TO_CIRCUIT_KEY.get(track)
        if not circuit_key or circuit_key not in SPRINT_CIRCUITS:
            continue
        driver = row.get("Driver", "").strip()
        if driver not in driver_names:
            continue
        try:
            pos = int(row.get("Position", "0"))
        except (ValueError, TypeError):
            continue
        sq_positions.setdefault(circuit_key, {})[driver] = pos
        for qkey in ("Q3", "Q2", "Q1"):
            t = _parse_quali_time(row.get(qkey, ""))
            if t is not None:
                sq_times.setdefault(circuit_key, {})[driver] = t
                break
    return sq_positions, sq_times


def fetch_csv_race_results(year=2026, progress_callback=None):
    """Fetch race results from formula1-datasets CSV.

    Returns:
        race_results: {circuit_key: [{driver, position, points, grid}, ...]}
        csv_standings: [{driver, points, position}, ...] — cumulative standings
    """
    filename = F1_DATASETS_FILES["race_results"].format(year=year)
    url = f"{F1_DATASETS_BASE_URL}/{filename}"
    if progress_callback:
        progress_callback("Cross-referencing race results (CSV)...")
    text = _fetch_raw_text(url)
    if text is None:
        return {}, []
    driver_names = {d["name"] for d in DRIVERS_2026}
    race_results = {}
    points_tally = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        track = row.get("Track", "").strip()
        circuit_key = CSV_TRACK_TO_CIRCUIT_KEY.get(track)
        if not circuit_key:
            continue
        driver = row.get("Driver", "").strip()
        if driver not in driver_names:
            continue
        try:
            pos = int(row.get("Position", "0"))
        except (ValueError, TypeError):
            pos = None
        try:
            pts = int(row.get("Points", "0"))
        except (ValueError, TypeError):
            pts = 0
        try:
            grid = int(row.get("Starting Grid", "0"))
        except (ValueError, TypeError):
            grid = 0
        race_results.setdefault(circuit_key, []).append({
            "driver": driver,
            "position": pos,
            "points": pts,
            "grid": grid,
        })
        points_tally[driver] = points_tally.get(driver, 0) + pts

    # Build cumulative standings sorted by points descending
    csv_standings = sorted(
        [{"driver": d, "points": p} for d, p in points_tally.items()],
        key=lambda x: x["points"], reverse=True,
    )
    for i, entry in enumerate(csv_standings, 1):
        entry["position"] = i

    return race_results, csv_standings


def fetch_csv_sprint_results(year=2026, progress_callback=None):
    """Fetch sprint results from formula1-datasets CSV.

    Returns:
        sprint_results: {circuit_key: [{driver, position, points, grid}, ...]}
        sprint_points: {driver: total_sprint_points}
    """
    filename = F1_DATASETS_FILES["sprint_results"].format(year=year)
    url = f"{F1_DATASETS_BASE_URL}/{filename}"
    if progress_callback:
        progress_callback("Cross-referencing sprint results (CSV)...")
    text = _fetch_raw_text(url)
    if text is None:
        return {}, {}
    driver_names = {d["name"] for d in DRIVERS_2026}
    sprint_results = {}
    sprint_points = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        track = row.get("Track", "").strip()
        circuit_key = CSV_TRACK_TO_CIRCUIT_KEY.get(track)
        if not circuit_key or circuit_key not in SPRINT_CIRCUITS:
            continue
        driver = row.get("Driver", "").strip()
        if driver not in driver_names:
            continue
        try:
            pos = int(row.get("Position", "0"))
        except (ValueError, TypeError):
            pos = None
        try:
            pts = int(row.get("Points", "0"))
        except (ValueError, TypeError):
            pts = 0
        try:
            grid = int(row.get("Starting Grid", "0"))
        except (ValueError, TypeError):
            grid = 0
        sprint_results.setdefault(circuit_key, []).append({
            "driver": driver,
            "position": pos,
            "points": pts,
            "grid": grid,
        })
        sprint_points[driver] = sprint_points.get(driver, 0) + pts
    return sprint_results, sprint_points


def _parse_race_time_to_ms(time_str: str) -> int | None:
    """Parse a race time string like '1:23:06.801' to milliseconds."""
    if not time_str:
        return None
    parts = time_str.split(':')
    try:
        if len(parts) == 3:
            total = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            total = int(parts[0]) * 60 + float(parts[1])
        else:
            return None
        return int(total * 1000)
    except (ValueError, TypeError):
        return None


def fetch_raw_season_data(year=2026, historical=None, progress_callback=None) -> dict:
    """Fetch latest season data from f1db raw YAML files on GitHub main branch.

    Used as fallback when the f1db SQLite release is not yet updated with
    recent race results.
    """
    result = {
        "driver_standings": [],
        "constructor_standings": [],
        "latest_race": None,
        "season_calendar": [],
        "quali_results": {},
        "quali_times": {},
        "sprint_quali_results": {},
        "sprint_quali_times": {},
        "session_completion": {},
        "live_fp_times": {},
        "calibration_correction": 1.0,
        "calibration_races": 0,
    }

    # Fetch championship standings
    if progress_callback:
        progress_callback("Fetching standings from f1db GitHub...")
    ds_text = _fetch_raw_text(f"{F1DB_RAW_BASE_URL}/{year}/driver-standings.yml")
    if ds_text:
        for entry in _parse_simple_yaml_list(ds_text):
            driver_name = _F1DB_TO_DRIVER.get(
                entry.get("driverId"), entry.get("driverId", ""))
            result["driver_standings"].append({
                "position": entry.get("position"),
                "driver": driver_name,
                "driver_id": entry.get("driverId"),
                "points": float(entry.get("points", 0) or 0),
            })

    cs_text = _fetch_raw_text(f"{F1DB_RAW_BASE_URL}/{year}/constructor-standings.yml")
    if cs_text:
        for entry in _parse_simple_yaml_list(cs_text):
            result["constructor_standings"].append({
                "position": entry.get("position"),
                "constructor_id": entry.get("constructorId"),
                "points": float(entry.get("points", 0) or 0),
            })

    # Check each round in order for completed race results
    completed = {}
    sorted_circuits = sorted(RACE_DIR_SLUGS.items(),
                             key=lambda x: int(x[1].split('-')[0]))
    for circuit_key, slug in sorted_circuits:
        round_num = int(slug.split('-')[0])
        if progress_callback:
            progress_callback(f"Checking R{round_num:02d}...")

        race_text = _fetch_raw_text(
            f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/race-results.yml")
        if race_text is None:
            break  # Races are sequential; stop at first incomplete round

        entries = _parse_simple_yaml_list(race_text)
        if not entries:
            break

        race_results = []
        winner_time_ms = None
        for entry in entries:
            driver_name = _F1DB_TO_DRIVER.get(
                entry.get("driverId"), entry.get("driverId", ""))
            time_str = entry.get("time")
            time_ms = _parse_race_time_to_ms(time_str) if time_str else None
            pos = entry.get("position")
            if pos == 1 and time_ms:
                winner_time_ms = time_ms
            race_results.append({
                "position": pos,
                "driver": driver_name,
                "driver_id": entry.get("driverId"),
                "constructor_id": entry.get("constructorId"),
                "time": time_str,
                "points": float(entry.get("points", 0) or 0),
                "fastest_lap": False,
            })

        completed[circuit_key] = {
            "round": round_num,
            "grand_prix": slug.split('-', 1)[1],
            "circuit_id": CIRCUIT_F1DB_IDS.get(circuit_key, ""),
            "date": "",
            "results": race_results,
            "winner_time_ms": winner_time_ms,
        }

        # Fetch qualifying for this completed round
        quali_text = _fetch_raw_text(
            f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/qualifying-results.yml")
        if quali_text:
            quali_map = {}
            quali_times_map = {}
            for entry in _parse_simple_yaml_list(quali_text):
                dn = _F1DB_TO_DRIVER.get(entry.get("driverId"))
                if dn and entry.get("position") is not None:
                    quali_map[dn] = entry["position"]
                # Extract best qualifying time (prefer q3 > q2 > q1)
                if dn:
                    best_t = None
                    for qkey in ("q3", "q2", "q1"):
                        t = _parse_quali_time(str(entry.get(qkey, "")))
                        if t is not None:
                            best_t = t
                            break
                    if best_t is not None:
                        quali_times_map[dn] = best_t
            if quali_map:
                result["quali_results"][circuit_key] = quali_map
            if quali_times_map:
                result["quali_times"][circuit_key] = quali_times_map

        # Fetch sprint qualifying for sprint rounds
        if circuit_key in SPRINT_CIRCUITS:
            sq_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/sprint-qualifying-results.yml")
            if sq_text:
                sq_map = {}
                sq_times_map = {}
                for entry in _parse_simple_yaml_list(sq_text):
                    dn = _F1DB_TO_DRIVER.get(entry.get("driverId"))
                    if dn and entry.get("position") is not None:
                        sq_map[dn] = entry["position"]
                    if dn:
                        best_t = None
                        for qkey in ("q3", "q2", "q1"):
                            t = _parse_quali_time(str(entry.get(qkey, "")))
                            if t is not None:
                                best_t = t
                                break
                        if best_t is not None:
                            sq_times_map[dn] = best_t
                if sq_map:
                    result["sprint_quali_results"][circuit_key] = sq_map
                if sq_times_map:
                    result["sprint_quali_times"][circuit_key] = sq_times_map

        # Probe session completion for this completed round
        session_types = set()
        session_types.add("RACE_RESULT")  # race_text was non-None to reach here
        if quali_text:
            session_types.add("QUALIFYING_RESULT")
        if circuit_key in SPRINT_CIRCUITS and circuit_key in result["sprint_quali_results"]:
            session_types.add("SPRINT_QUALIFYING_RESULT")

        fp1_text = _fetch_raw_text(
            f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-1-results.yml")
        if fp1_text:
            session_types.add("FREE_PRACTICE_1_RESULT")

        if circuit_key not in SPRINT_CIRCUITS:
            fp2_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-2-results.yml")
            if fp2_text:
                session_types.add("FREE_PRACTICE_2_RESULT")
            fp3_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-3-results.yml")
            if fp3_text:
                session_types.add("FREE_PRACTICE_3_RESULT")
        else:
            sr_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/sprint-race-results.yml")
            if sr_text:
                session_types.add("SPRINT_RACE_RESULT")

        result["session_completion"][circuit_key] = session_types

        # Parse live FP times for this completed round
        fp_data = {}  # {driver_name: {"fp1": secs, "fp2": secs, "fp3": secs}}
        if fp1_text:
            for dn, t in _parse_fp_session_times(fp1_text).items():
                fp_data.setdefault(dn, {})["fp1"] = t
        if circuit_key not in SPRINT_CIRCUITS:
            if fp2_text:
                for dn, t in _parse_fp_session_times(fp2_text).items():
                    fp_data.setdefault(dn, {})["fp2"] = t
            if fp3_text:
                for dn, t in _parse_fp_session_times(fp3_text).items():
                    fp_data.setdefault(dn, {})["fp3"] = t
        if fp_data:
            result["live_fp_times"][circuit_key] = fp_data

    # Probe upcoming rounds for partial session data (FP1/FP2/FP3/SQ/Q)
    for circuit_key, slug in sorted_circuits:
        if circuit_key in result["session_completion"]:
            continue  # Already probed as a completed round
        session_types = set()
        fp1_text = _fetch_raw_text(
            f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-1-results.yml")
        if fp1_text:
            session_types.add("FREE_PRACTICE_1_RESULT")
        else:
            continue  # If no FP1 yet, later sessions won't exist either
        fp2_text = None
        fp3_text = None
        if circuit_key not in SPRINT_CIRCUITS:
            fp2_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-2-results.yml")
            if fp2_text:
                session_types.add("FREE_PRACTICE_2_RESULT")
            fp3_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/free-practice-3-results.yml")
            if fp3_text:
                session_types.add("FREE_PRACTICE_3_RESULT")
        else:
            sq_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/sprint-qualifying-results.yml")
            if sq_text:
                session_types.add("SPRINT_QUALIFYING_RESULT")
            sr_text = _fetch_raw_text(
                f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/sprint-race-results.yml")
            if sr_text:
                session_types.add("SPRINT_RACE_RESULT")
        quali_text = _fetch_raw_text(
            f"{F1DB_RAW_BASE_URL}/{year}/races/{slug}/qualifying-results.yml")
        if quali_text:
            session_types.add("QUALIFYING_RESULT")
        if session_types:
            result["session_completion"][circuit_key] = session_types

        # Parse live FP times for this upcoming round
        fp_data = {}
        if fp1_text:
            for dn, t in _parse_fp_session_times(fp1_text).items():
                fp_data.setdefault(dn, {})["fp1"] = t
        if circuit_key not in SPRINT_CIRCUITS:
            if fp2_text:
                for dn, t in _parse_fp_session_times(fp2_text).items():
                    fp_data.setdefault(dn, {})["fp2"] = t
            if fp3_text:
                for dn, t in _parse_fp_session_times(fp3_text).items():
                    fp_data.setdefault(dn, {})["fp3"] = t
        if fp_data:
            result["live_fp_times"][circuit_key] = fp_data

    # Set latest race (highest completed round)
    if completed:
        latest_key = max(completed, key=lambda k: completed[k]["round"])
        result["latest_race"] = completed[latest_key]

    # Build full calendar
    for circuit_key, slug in RACE_DIR_SLUGS.items():
        round_num = int(slug.split('-')[0])
        result["season_calendar"].append({
            "round": round_num,
            "date": "",
            "grand_prix": slug.split('-', 1)[1],
            "circuit_id": CIRCUIT_F1DB_IDS.get(circuit_key, ""),
            "is_sprint": circuit_key in SPRINT_CIRCUITS,
            "completed": circuit_key in completed,
        })

    # Compute auto-calibration from completed race winner times
    if completed:
        global GLOBAL_CORRECTION
        with _GC_LOCK:
            saved_gc = GLOBAL_CORRECTION
            GLOBAL_CORRECTION = 1.0
        ratios = []
        for circuit_key, race_data in completed.items():
            if race_data.get("winner_time_ms") is None:
                continue
            actual_seconds = race_data["winner_time_ms"] / 1000.0
            projections = calculate_all_projections(circuit_key, historical)
            if not projections:
                continue
            projected_seconds = projections[0]["time_s"]
            if projected_seconds > 0:
                ratios.append(actual_seconds / projected_seconds)
        with _GC_LOCK:
            GLOBAL_CORRECTION = saved_gc
        if ratios:
            result["calibration_correction"] = round(
                sum(ratios) / len(ratios), 6)
            result["calibration_races"] = len(ratios)

    return result


def compute_historical_factor(driver_name, circuit_key, historical):
    """Compute historical performance factor for a driver at a circuit.

    Returns a value in [0.98, 1.02]. Values < 1.0 mean the driver
    historically performs better than their average at this circuit.
    """
    alpha = DRIVER_CATEGORIES.get(driver_name, {}).get("alpha", 0.0)
    if alpha == 0.0:
        return 1.0  # No historical influence

    driver_data = historical.get(driver_name, {})
    if not driver_data:
        return 1.0  # No data available

    circuit_data = driver_data.get(circuit_key)
    if circuit_data is None or circuit_data["races"] == 0:
        return 1.0  # Never raced at this circuit

    # Compute overall average finish across all circuits
    all_finishes = [d["avg_finish"] for d in driver_data.values()
                    if d["races"] > 0]
    if not all_finishes:
        return 1.0

    overall_avg = sum(all_finishes) / len(all_finishes)
    circuit_avg = circuit_data["avg_finish"]

    # Circuit affinity: ratio of circuit performance to overall
    # Lower avg_finish = better, so invert: overall/circuit
    if circuit_avg == 0 or overall_avg == 0:
        return 1.0

    circuit_affinity = circuit_avg / overall_avg
    deviation = circuit_affinity - 1.0

    # Apply alpha-weighted deviation and clamp
    factor = 1.0 + (deviation * alpha * 0.01)
    return max(0.98, min(1.02, factor))


def compute_driver_dnf_probability(driver_name, circuit_key, historical):
    """Compute per-driver DNF probability for a specific circuit.

    Blends team baseline reliability with the driver's individual historical
    DNF rate at this circuit (and overall). Returns a probability in [0.0, 1.0].
    """
    # Find driver's team tag
    driver_info = next((d for d in DRIVERS_2026 if d["name"] == driver_name), None)
    if driver_info is None:
        return 0.08  # Fallback grid average

    team_baseline = TEAM_DNF_PROBABILITY.get(driver_info["tag"], 0.08)

    if not historical:
        return team_baseline

    driver_data = historical.get(driver_name, {})
    if not driver_data:
        return team_baseline

    # Compute driver's overall historical DNF rate
    total_races = sum(d["races"] for d in driver_data.values() if d["races"] > 0)
    total_dnfs = sum(d["dnfs"] for d in driver_data.values())
    if total_races == 0:
        return team_baseline

    overall_dnf_rate = total_dnfs / total_races

    # Circuit-specific DNF rate (if the driver has history at this circuit)
    circuit_data = driver_data.get(circuit_key)
    if circuit_data and circuit_data["races"] >= 2:
        circuit_dnf_rate = circuit_data["dnfs"] / circuit_data["races"]
        # Blend: 40% circuit-specific, 30% overall driver, 30% team baseline
        blended = 0.40 * circuit_dnf_rate + 0.30 * overall_dnf_rate + 0.30 * team_baseline
    else:
        # No circuit-specific data: 50% driver overall, 50% team baseline
        blended = 0.50 * overall_dnf_rate + 0.50 * team_baseline

    # Clamp to reasonable range
    return max(0.01, min(0.30, blended))


def calculate_expected_points(projections, overtaking_factor, points_table=None):
    """Calculate expected points accounting for overtaking difficulty and DNF probability.

    At circuits where overtaking is difficult, position uncertainty is higher:
    a fast car stuck behind a slower one cannot easily recover. This function
    blends each driver's projected-position points with neighboring positions
    weighted by the overtaking difficulty.

    Args:
        projections: List of projection dicts (already sorted by time_s).
        overtaking_factor: 0.0 (impossible) to 1.0 (very easy).
        points_table: Points dict mapping position to points (default: RACE_POINTS).

    Returns:
        List of dicts with keys 'exp_pts', 'pos_low', 'pos_high'
        (same order as projections).
    """
    if points_table is None:
        points_table = RACE_POINTS
    n = len(projections)
    # Position uncertainty bandwidth: how many positions a driver might
    # realistically shift due to non-pace factors. Lower overtaking = wider band.
    # At overtaking=1.0: bandwidth=0 (pure pace). At overtaking=0.0: bandwidth=3.
    bandwidth = (1.0 - overtaking_factor) * 3.0

    expected = []
    for i in range(n):
        pos = i + 1
        base_pts = points_table.get(pos, 0)

        if bandwidth < 0.1:
            # High-overtaking circuit: pace fully determines position
            expected.append({"exp_pts": base_pts, "pos_low": pos, "pos_high": pos})
            continue

        # Gaussian-weighted blend of nearby positions
        total_weight = 0.0
        weighted_pts = 0.0
        sigma = bandwidth  # Standard deviation in positions

        for j in range(n):
            neighbor_pos = j + 1
            neighbor_pts = points_table.get(neighbor_pos, 0)
            dist = abs(i - j)
            # Gaussian weight: exp(-(dist^2) / (2 * sigma^2))
            weight = math.exp(-(dist ** 2) / (2 * sigma ** 2))
            weighted_pts += weight * neighbor_pts
            total_weight += weight

        blended_pts = weighted_pts / total_weight if total_weight > 0 else base_pts

        # Blend between pure-pace points and uncertainty-adjusted points
        # overtaking_factor=1.0 → 100% pace-based; 0.0 → 100% blended
        final_pts = overtaking_factor * base_pts + (1.0 - overtaking_factor) * blended_pts

        # Compute P10-P90 position range from the Gaussian weights
        weights = []
        for j in range(n):
            dist = abs(i - j)
            w = math.exp(-(dist ** 2) / (2 * sigma ** 2))
            weights.append((j + 1, w))

        total_w = sum(w for _, w in weights)
        weights_norm = [(p_pos, w / total_w) for p_pos, w in weights]
        weights_norm.sort(key=lambda x: x[0])

        cumulative = 0.0
        pos_low = pos
        pos_high = pos
        found_low = False
        for p_pos, w in weights_norm:
            cumulative += w
            if not found_low and cumulative >= 0.10:
                pos_low = p_pos
                found_low = True
            if cumulative >= 0.90:
                pos_high = p_pos
                break

        expected.append({"exp_pts": final_pts, "pos_low": pos_low, "pos_high": pos_high})

    return expected


# ---------------------------------------------------------------------------
# Phase 2 – Projection Algorithm
# ---------------------------------------------------------------------------


# Formatters (defined first; called by projection functions below)
def format_lap_time(seconds: float) -> str:
    """Format seconds into M:SS.mmm display string."""
    minutes = int(seconds // 60)
    remaining = seconds - (minutes * 60)
    return f"{minutes}:{remaining:06.3f}"


def format_race_time(seconds: float) -> str:
    """Format seconds into H:MM:SS.mmm display string for total race time."""
    hours = int(seconds // 3600)
    remaining = seconds - (hours * 3600)
    minutes = int(remaining // 60)
    secs = remaining - (minutes * 60)
    return f"{hours}:{minutes:02d}:{secs:06.3f}"


def format_gap(gap_seconds: float) -> str:
    """Format a gap for display - seconds or M:SS for larger values."""
    if gap_seconds == 0:
        return "LEADER"
    if gap_seconds >= 60:
        minutes = int(gap_seconds // 60)
        secs = gap_seconds - (minutes * 60)
        return f"+{minutes}:{secs:06.3f}"
    return f"+{gap_seconds:.3f}s"


# Full-grid projection for a given circuit - returns projected total race times
def calculate_all_projections(circuit_key: str, historical: dict | None = None,
                              quali_positions: dict | None = None,
                              live_fp_data: dict | None = None) -> list[dict]:
    """Calculate projected total race times for all drivers at the given circuit.

    Projection priority per driver:
    1. Live FP data from this circuit (used directly, no ratio scaling).
    2. Albert Park baselines (only when circuit_key == 'albert_park').
    3. Circuit reference time from REFERENCE_LAP_TIME × ratio, differentiated
       only by historical factor and team affinity.
    """
    circuit = CIRCUITS_2026[circuit_key]
    target_ratio = circuit["ratio"]
    circuit_type = circuit["type"]
    race_laps = circuit["laps"]
    overtaking = CIRCUIT_OVERTAKING.get(circuit_key, 0.50)
    results = []

    for driver in DRIVERS_2026:
        affinity = TEAM_AFFINITIES.get(driver["tag"], {}).get(circuit_type, 1.0)
        hist_factor = compute_historical_factor(driver["name"], circuit_key, historical) if historical else 1.0
        dnf_prob = compute_driver_dnf_probability(driver["name"], circuit_key, historical)

        # Use live FP data when available (times already at this circuit)
        live_driver = (live_fp_data or {}).get(driver["name"])
        if live_driver:
            baseline = compute_composite_baseline(live_driver)
            projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
        elif circuit_key == "albert_park":
            baseline = compute_composite_baseline(driver)
            projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
        else:
            # No circuit-specific FP data — use circuit reference + historical/affinity
            circuit_ref = REFERENCE_LAP_TIME * target_ratio
            projected_lap = circuit_ref * affinity * GLOBAL_CORRECTION * hist_factor
        total_race = projected_lap * race_laps
        results.append({
            "driver": driver["name"],
            "num": driver["num"],
            "team": driver["team"],
            "tag": driver["tag"],
            "data_quality": driver.get("data_quality", "measured"),
            "time_s": total_race,
            "time_str": format_race_time(total_race),
            "lap_s": projected_lap,
            "lap_str": format_lap_time(projected_lap),
            "hist_factor": hist_factor,
            "dnf_prob": dnf_prob,
        })

    results.sort(key=lambda r: r["time_s"])

    leader_time = results[0]["time_s"]
    for r in results:
        r["gap"] = round(r["time_s"] - leader_time, 3)
        r["gap_str"] = format_gap(r["gap"])

    for i, r in enumerate(results):
        r["proj_pts"] = RACE_POINTS.get(i + 1, 0)

    # If qualifying data available, apply grid-position blend
    if quali_positions:
        quali_weight = 0.60  # 60% qualifying, 40% pace model
        n = len(results)
        pace_ranks = {r["driver"]: i for i, r in enumerate(results)}

        for r in results:
            quali_pos = quali_positions.get(r["driver"])
            if quali_pos is not None:
                pace_rank = pace_ranks[r["driver"]]
                blended_rank = quali_weight * (quali_pos - 1) + (1 - quali_weight) * pace_rank
                r["_sort_key"] = blended_rank
                r["quali_pos"] = quali_pos
            else:
                r["_sort_key"] = pace_ranks[r["driver"]]
                r["quali_pos"] = None

        results.sort(key=lambda r: r["_sort_key"])

        leader_time = results[0]["time_s"]
        for r in results:
            r["gap"] = round(r["time_s"] - leader_time, 3)
            r["gap_str"] = format_gap(r["gap"])

        for i, r in enumerate(results):
            r["proj_pts"] = RACE_POINTS.get(i + 1, 0)

        for r in results:
            r.pop("_sort_key", None)
    else:
        for r in results:
            r["quali_pos"] = None

    # Track mode
    for r in results:
        r["quali_mode"] = bool(quali_positions)

    # Compute expected points using overtaking difficulty and DNF probability
    exp_pts_list = calculate_expected_points(results, overtaking)
    for i, r in enumerate(results):
        ep = exp_pts_list[i]
        # E[Pts] = position-uncertainty-adjusted points × probability of finishing
        r["exp_pts"] = round(ep["exp_pts"] * (1.0 - r["dnf_prob"]), 2)
        r["pos_low"] = ep["pos_low"]
        r["pos_high"] = ep["pos_high"]
        r["dnf_pct"] = f"{r['dnf_prob'] * 100:.0f}%"

    return results


def _parse_quali_time(time_str: str) -> float | None:
    """Parse a qualifying time string like '1:18.518' to seconds."""
    if not time_str:
        return None
    parts = time_str.split(':')
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return float(parts[0])
    except (ValueError, TypeError):
        return None


def _parse_fp_session_times(fp_text: str) -> dict[str, float]:
    """Parse free-practice results YAML and return {driver_name: best_time_seconds}."""
    times = {}
    for entry in _parse_simple_yaml_list(fp_text):
        dn = _F1DB_TO_DRIVER.get(entry.get("driverId"))
        if not dn:
            continue
        t = _parse_quali_time(str(entry.get("time", "")))
        if t is not None:
            times[dn] = t
    return times


def calculate_qualifying_projections(circuit_key: str,
                                     quali_positions: dict,
                                     quali_times: dict | None = None,
                                     historical: dict | None = None,
                                     live_fp_data: dict | None = None) -> list[dict]:
    """Calculate projected race outcome based on qualifying results and historical data.

    In qualifying mode, the grid order is primary. If qualifying lap times
    are available, they're used to project race times scaled by the circuit's
    lap count and a quali-to-race degradation factor. Otherwise, the practice
    composite baseline is used but ordered by qualifying position.
    When live_fp_data is available, FP-based fallback uses circuit-specific times.
    """
    circuit = CIRCUITS_2026[circuit_key]
    target_ratio = circuit["ratio"]
    circuit_type = circuit["type"]
    race_laps = circuit["laps"]
    overtaking = CIRCUIT_OVERTAKING.get(circuit_key, 0.50)

    # Albert Park qualifying baseline ratio (quali times are faster than race pace)
    # Typical race lap is ~5-7% slower than qualifying lap
    QUALI_TO_RACE_FACTOR = 1.06

    results = []
    driver_lookup = {d["name"]: d for d in DRIVERS_2026}

    for driver in DRIVERS_2026:
        quali_pos = quali_positions.get(driver["name"])
        hist_factor = (compute_historical_factor(driver["name"], circuit_key, historical)
                       if historical else 1.0)
        dnf_prob = compute_driver_dnf_probability(driver["name"], circuit_key, historical)
        affinity = TEAM_AFFINITIES.get(driver["tag"], {}).get(circuit_type, 1.0)

        # Determine lap time baseline
        q_time = (quali_times or {}).get(driver["name"])
        if q_time is not None:
            # Use actual qualifying time, scaled to race pace
            projected_lap = q_time * QUALI_TO_RACE_FACTOR * affinity * GLOBAL_CORRECTION * hist_factor
        else:
            # Fallback to practice composite — prefer live circuit-specific FP data
            live_driver = (live_fp_data or {}).get(driver["name"])
            if live_driver:
                baseline = compute_composite_baseline(live_driver)
                projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
            elif circuit_key == "albert_park":
                baseline = compute_composite_baseline(driver)
                projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
            else:
                circuit_ref = REFERENCE_LAP_TIME * target_ratio
                projected_lap = circuit_ref * affinity * GLOBAL_CORRECTION * hist_factor

        total_race = projected_lap * race_laps

        results.append({
            "driver": driver["name"],
            "num": driver["num"],
            "team": driver["team"],
            "tag": driver["tag"],
            "data_quality": driver.get("data_quality", "measured"),
            "time_s": total_race,
            "time_str": format_race_time(total_race),
            "lap_s": projected_lap,
            "lap_str": format_lap_time(projected_lap),
            "hist_factor": hist_factor,
            "dnf_prob": dnf_prob,
            "quali_pos": quali_pos,
        })

    # Sort by qualifying position (drivers without a quali position go to the back)
    results.sort(key=lambda r: (r["quali_pos"] if r["quali_pos"] is not None else 99,
                                r["time_s"]))

    leader_time = results[0]["time_s"]
    for r in results:
        r["gap"] = round(r["time_s"] - leader_time, 3)
        r["gap_str"] = format_gap(r["gap"])

    for i, r in enumerate(results):
        r["proj_pts"] = RACE_POINTS.get(i + 1, 0)

    for r in results:
        r["quali_mode"] = True

    # Compute expected points using overtaking difficulty and DNF probability
    exp_pts_list = calculate_expected_points(results, overtaking)
    for i, r in enumerate(results):
        ep = exp_pts_list[i]
        r["exp_pts"] = round(ep["exp_pts"] * (1.0 - r["dnf_prob"]), 2)
        r["pos_low"] = ep["pos_low"]
        r["pos_high"] = ep["pos_high"]
        r["dnf_pct"] = f"{r['dnf_prob'] * 100:.0f}%"

    return results


def calculate_sprint_projections(circuit_key: str,
                                 sq_positions: dict,
                                 sq_times: dict | None = None,
                                 historical: dict | None = None,
                                 live_fp_data: dict | None = None) -> list[dict]:
    """Calculate projected sprint race outcome based on sprint qualifying data.

    Uses SQ times when available, falls back to FP1 composite baseline.
    Sprint-specific: shorter distance, lower degradation, reduced DNF probability.
    Returns empty list if the circuit does not host a sprint race.
    """
    sprint_info = SPRINT_CIRCUITS.get(circuit_key)
    if not sprint_info:
        return []

    sprint_laps = sprint_info["sprint_laps"]
    circuit = CIRCUITS_2026[circuit_key]
    target_ratio = circuit["ratio"]
    circuit_type = circuit["type"]
    overtaking = CIRCUIT_OVERTAKING.get(circuit_key, 0.50)

    results = []
    for driver in DRIVERS_2026:
        sq_pos = sq_positions.get(driver["name"])
        hist_factor = (compute_historical_factor(driver["name"], circuit_key, historical)
                       if historical else 1.0)
        race_dnf = compute_driver_dnf_probability(driver["name"], circuit_key, historical)
        sprint_dnf = race_dnf * SPRINT_DNF_FACTOR
        affinity = TEAM_AFFINITIES.get(driver["tag"], {}).get(circuit_type, 1.0)

        sq_time = (sq_times or {}).get(driver["name"])
        if sq_time is not None:
            projected_lap = sq_time * SQ_TO_SPRINT_FACTOR * affinity * GLOBAL_CORRECTION * hist_factor
        else:
            # Fallback — prefer live circuit-specific FP data
            live_driver = (live_fp_data or {}).get(driver["name"])
            if live_driver:
                baseline = compute_composite_baseline(live_driver)
                projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
            elif circuit_key == "albert_park":
                baseline = compute_composite_baseline(driver)
                projected_lap = baseline * affinity * GLOBAL_CORRECTION * hist_factor
            else:
                circuit_ref = REFERENCE_LAP_TIME * target_ratio
                projected_lap = circuit_ref * affinity * GLOBAL_CORRECTION * hist_factor

        total_time = projected_lap * sprint_laps

        results.append({
            "driver": driver["name"],
            "num": driver["num"],
            "team": driver["team"],
            "tag": driver["tag"],
            "data_quality": driver.get("data_quality", "measured"),
            "time_s": total_time,
            "time_str": format_race_time(total_time),
            "lap_s": projected_lap,
            "lap_str": format_lap_time(projected_lap),
            "hist_factor": hist_factor,
            "dnf_prob": sprint_dnf,
            "quali_pos": sq_pos,
        })

    # Sort by SQ position (grid order); drivers without position sorted by time
    results.sort(key=lambda r: (r["quali_pos"] if r["quali_pos"] is not None else 99,
                                r["time_s"]))

    leader_time = results[0]["time_s"]
    for r in results:
        r["gap"] = round(r["time_s"] - leader_time, 3)
        r["gap_str"] = format_gap(r["gap"])

    for i, r in enumerate(results):
        r["proj_pts"] = SPRINT_POINTS.get(i + 1, 0)

    for r in results:
        r["sprint_mode"] = True

    # Expected points using SPRINT_POINTS table
    exp_pts_list = calculate_expected_points(results, overtaking, points_table=SPRINT_POINTS)
    for i, r in enumerate(results):
        ep = exp_pts_list[i]
        r["exp_pts"] = round(ep["exp_pts"] * (1.0 - r["dnf_prob"]), 2)
        r["pos_low"] = ep["pos_low"]
        r["pos_high"] = ep["pos_high"]
        r["dnf_pct"] = f"{r['dnf_prob'] * 100:.0f}%"

    return results


def calculate_season_projection(historical: dict | None = None,
                                actual_standings: list | None = None,
                                season_calendar: list | None = None,
                                live_fp_times: dict | None = None) -> list[dict]:
    """Project full-season championship standings.

    Sums expected points across all 24 circuits. For completed races (when
    actual_standings is provided), uses actual points; for future races, uses E[Pts].

    Returns list of dicts sorted by total projected points:
    [{driver, team, tag, actual_pts, projected_pts, total_pts, races_projected}]
    """
    # Initialize per-driver accumulator
    driver_totals = {}
    for driver in DRIVERS_2026:
        driver_totals[driver["name"]] = {
            "driver": driver["name"],
            "team": driver["team"],
            "tag": driver["tag"],
            "actual_pts": 0.0,
            "projected_pts": 0.0,
            "races_projected": 0,
        }

    # Get actual points from standings if available
    if actual_standings:
        for s in actual_standings:
            if s["driver"] in driver_totals:
                driver_totals[s["driver"]]["actual_pts"] = s["points"]

    # Determine which circuits are already completed
    completed_circuits = set()
    if season_calendar:
        # Build reverse mapping: f1db circuit_id -> app circuit_key
        f1db_to_key = {v: k for k, v in CIRCUIT_F1DB_IDS.items()}
        for entry in season_calendar:
            if entry.get("completed"):
                ck = f1db_to_key.get(entry["circuit_id"])
                if ck:
                    completed_circuits.add(ck)

    # Project remaining races
    for circuit_key in CIRCUITS_2026:
        if circuit_key in completed_circuits:
            continue
        fp_data = (live_fp_times or {}).get(circuit_key)
        projections = calculate_all_projections(circuit_key, historical, live_fp_data=fp_data)
        for p in projections:
            if p["driver"] in driver_totals:
                driver_totals[p["driver"]]["projected_pts"] += p["exp_pts"]
                driver_totals[p["driver"]]["races_projected"] += 1

    # Project sprint races for remaining sprint circuits
    for circuit_key in SPRINT_CIRCUITS:
        if circuit_key in completed_circuits:
            continue
        fp_data = (live_fp_times or {}).get(circuit_key)
        sprint_proj = calculate_sprint_projections(circuit_key, {}, None, historical, fp_data)
        for sp in sprint_proj:
            if sp["driver"] in driver_totals:
                driver_totals[sp["driver"]]["projected_pts"] += sp["exp_pts"]

    # Combine actual + projected
    result = []
    for dt in driver_totals.values():
        dt["total_pts"] = round(dt["actual_pts"] + dt["projected_pts"], 1)
        dt["projected_pts"] = round(dt["projected_pts"], 1)
        result.append(dt)

    result.sort(key=lambda r: r["total_pts"], reverse=True)
    return result


# --- GUI Code (Phase 3) ---


class F1ProjectionApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("F1 2026 - Race Time Projections")
        self.geometry("1150x1000")
        self.minsize(950, 700)
        self.configure(bg="#15151E")

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=0)  # Header
        self.rowconfigure(1, weight=0)  # Selector
        self.rowconfigure(2, weight=1)  # Table
        self.rowconfigure(3, weight=0)  # Footer
        self.rowconfigure(4, weight=0)  # Standings toggle
        self.rowconfigure(5, weight=0)  # Standings content
        self.rowconfigure(6, weight=0)  # Championship toggle
        self.rowconfigure(7, weight=0)  # Championship content

        # Phase 3 instance variables
        self.latest_race = None
        self.driver_standings = []
        self.constructor_standings = []
        self.season_calendar = []
        self.calibration_races = 0

        self.historical_data = None
        self.quali_results = {}  # circuit_key -> {driver_name: position}
        self.quali_times = {}    # circuit_key -> {driver_name: best_q_time_seconds}
        self.sprint_quali_results = {}  # circuit_key -> {driver_name: sq_position}
        self.sprint_quali_times = {}    # circuit_key -> {driver_name: best_sq_time_seconds}
        self.session_completion = {}  # circuit_key -> set of completed session type strings
        self.live_fp_times = {}  # circuit_key -> {driver_name: {"fp1": secs, "fp2": secs, "fp3": secs}}

        # Track collapsible section state
        self._collapsed = {}  # section_name -> bool

        self._configure_styles()
        self._build_header()
        self._build_selector()
        self._build_table()
        self._build_footer()
        self._build_standings()
        self._build_championship_projection()
        self._configure_team_tags()

        # Disable selector until data loads
        self.circuit_combo.configure(state="disabled")
        self._show_status("Loading historical data...")

        # Load all data in background thread
        threading.Thread(target=self._load_all_data, daemon=True).start()

    def _configure_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")

        style.configure("TFrame", background="#15151E")
        style.configure("Header.TLabel", background="#15151E", foreground="#E10600",
                         font=("Segoe UI", 20, "bold"))
        style.configure("Sub.TLabel", background="#15151E", foreground="#AAAAAA",
                         font=("Segoe UI", 11))
        style.configure("TLabel", background="#15151E", foreground="#FFFFFF",
                         font=("Segoe UI", 11))
        style.configure("Treeview", background="#2A2A3C", foreground="#FFFFFF",
                         rowheight=28, fieldbackground="#2A2A3C",
                         font=("Segoe UI", 10))
        style.configure("Treeview.Heading", background="#15151E", foreground="#E10600",
                         font=("Segoe UI", 10, "bold"))
        style.map("Treeview", background=[("selected", "#E10600")],
                  foreground=[("selected", "#FFFFFF")])
        style.configure("TCombobox", fieldbackground="#2A2A3C",
                         background="#3A3A4C", foreground="#FFFFFF",
                         font=("Segoe UI", 11))
        style.configure("TLabelframe", background="#15151E", foreground="#E10600")
        style.configure("TLabelframe.Label", background="#15151E", foreground="#E10600",
                         font=("Segoe UI", 11, "bold"))

    def _build_header(self):
        frame = ttk.Frame(self, padding=(20, 15, 20, 5))
        frame.grid(row=0, column=0, sticky="ew")
        ttk.Label(frame, text="F1 2026 RACE TIME PROJECTIONS", style="Header.TLabel").pack(anchor="w")
        ttk.Label(frame, text="Based on Australian GP FP1/FP2/FP3 data (weighted composite) · Historical: f1nsight-api-2 · Latest: f1db",
                  style="Sub.TLabel").pack(anchor="w")
        self.status_label = ttk.Label(frame, text="", style="Sub.TLabel")
        self.status_label.pack(anchor="w")
        self.latest_race_label = ttk.Label(frame, text="", style="Sub.TLabel",
                                            font=("Segoe UI", 10))
        self.latest_race_label.pack(anchor="w")
        self.calibration_label = ttk.Label(frame, text="Calibration: baseline (no races)",
                                            style="Sub.TLabel", font=("Segoe UI", 10))
        self.calibration_label.pack(anchor="w")

    def _build_selector(self):
        outer = ttk.Frame(self, padding=(20, 5, 20, 5))
        outer.grid(row=1, column=0, sticky="ew")

        frame = ttk.Frame(outer)
        frame.pack(fill="x")

        ttk.Label(frame, text="Select Grand Prix:").pack(side="left", padx=(0, 8))

        circuit_names = [c["name"] for c in CIRCUITS_2026.values()]
        self.circuit_combo = ttk.Combobox(frame, values=circuit_names, state="readonly", width=30)
        self.circuit_combo.pack(side="left", padx=(0, 15))
        self.circuit_combo.bind("<<ComboboxSelected>>", lambda e: self.on_circuit_selected())

        ttk.Label(frame, text="Mode:").pack(side="left", padx=(0, 5))
        self.mode_combo = ttk.Combobox(frame, values=["Grand Prix", "Grand Prix +Q", "Sprint"],
                                        state="readonly", width=12)
        self.mode_combo.current(0)
        self.mode_combo.pack(side="left", padx=(0, 15))
        self.mode_combo.bind("<<ComboboxSelected>>", lambda e: self.on_circuit_selected())

        self.circuit_info = ttk.Label(frame, text="", style="Sub.TLabel")
        self.circuit_info.pack(side="left")

        self.refresh_btn = tk.Button(frame, text="\u21bb Refresh Data", command=self._refresh_data,
                                     bg="#3A3A4C", fg="#FFFFFF", activebackground="#E10600",
                                     activeforeground="#FFFFFF", font=("Segoe UI", 10),
                                     relief="flat", padx=10, pady=2)
        self.refresh_btn.pack(side="right")

        self.session_status_label = ttk.Label(outer, text="", style="Sub.TLabel",
                                               font=("Segoe UI", 9))
        self.session_status_label.pack(anchor="w", pady=(2, 0))

    def _get_selected_circuit_key(self) -> str:
        idx = self.circuit_combo.current()
        keys = list(CIRCUITS_2026.keys())
        return keys[idx] if 0 <= idx < len(keys) else "albert_park"

    def _build_table(self):
        frame = ttk.Frame(self, padding=(20, 5, 20, 15))
        frame.grid(row=2, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        columns = ("pos", "driver", "team", "quali", "time", "gap", "dnf", "proj_pts", "exp_pts", "range")
        self.tree = ttk.Treeview(frame, columns=columns, show="headings",
                                  selectmode="browse", height=22)

        col_cfg = {
            "pos":      ("Pos",                60,  "center"),
            "driver":   ("Driver",             170, "w"),
            "team":     ("Team",               170, "w"),
            "quali":    ("Quali",               55,  "center"),
            "time":     ("Projected Race Time", 165, "center"),
            "gap":      ("Gap to Leader",       120, "center"),
            "dnf":      ("DNF%",                55,  "center"),
            "proj_pts": ("Pts",                 50,  "center"),
            "exp_pts":  ("E[Pts]",              65,  "center"),
            "range":    ("Range",              80,  "center"),
        }
        for cid, (heading, width, anchor) in col_cfg.items():
            self.tree.heading(cid, text=heading,
                              command=lambda c=cid: self._sort_by_column(c))
            self.tree.column(cid, width=width, anchor=anchor, minwidth=50)

        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

    def _configure_team_tags(self):
        for tag, colors in TEAM_COLORS.items():
            self.tree.tag_configure(tag, background=colors["bg"], foreground=colors["fg"])

    def _load_all_data(self):
        """Background thread: load historical data, then f1db latest data."""
        # Step 1: f1nsight historical data
        try:
            data = build_historical_data(
                progress_callback=lambda msg: self.after(0, self._show_status, msg)
            )
            if data:
                self.historical_data = data
        except Exception as exc:
            self.after(0, self._show_status, f"f1nsight error: {exc}")

        if self.historical_data is None:
            # Fallback: try f1db for historical data
            self.after(0, self._show_status, "f1nsight unavailable — trying f1db...")
            conn = get_db_connection()
            if conn:
                try:
                    data = build_historical_data_f1db(conn)
                    if data:
                        self.historical_data = data
                finally:
                    conn.close()

        # Schedule GUI update for historical data
        self.after(0, self._on_historical_loaded)

        # Step 2: f1db latest data (standings, calendar, calibration)
        self.after(0, self._show_status, "Loading latest season data...")
        conn = get_db_connection()
        if conn:
            try:
                self.driver_standings, self.constructor_standings = fetch_current_standings(conn)
                self.latest_race = fetch_latest_race_result(conn)
                self.season_calendar = fetch_season_calendar(conn)
                correction, races_used = compute_auto_calibration(conn)
                if races_used > 0:
                    global GLOBAL_CORRECTION
                    with _GC_LOCK:
                        GLOBAL_CORRECTION = correction
                    self.calibration_races = races_used
                # Preload qualifying results for all circuits
                for circuit_key in CIRCUITS_2026:
                    quali = fetch_qualifying_results(conn, circuit_key)
                    if quali:
                        self.quali_results[circuit_key] = quali
                    qt = fetch_qualifying_times(conn, circuit_key)
                    if qt:
                        self.quali_times[circuit_key] = qt
                # Sprint qualifying data (SQLite)
                for ck in SPRINT_CIRCUITS:
                    try:
                        sq_r = fetch_sprint_qualifying_results(conn, ck)
                        if sq_r:
                            self.sprint_quali_results[ck] = sq_r
                        sq_t = fetch_sprint_qualifying_times(conn, ck)
                        if sq_t:
                            self.sprint_quali_times[ck] = sq_t
                    except Exception as exc:
                        self.after(0, self._show_status, f"Sprint quali load error ({ck}): {exc}")
                # Session completion data (SQLite)
                self.session_completion = fetch_session_completion(conn)
            finally:
                conn.close()

        # Step 3: If SQLite lacked standings, supplement from raw GitHub YAML
        if not self.driver_standings:
            self.after(0, self._show_status, "Fetching latest results from f1db GitHub...")
            try:
                raw = fetch_raw_season_data(
                    2026, self.historical_data,
                    lambda msg: self.after(0, self._show_status, msg))
                if raw.get("driver_standings"):
                    self.driver_standings = raw["driver_standings"]
                if raw.get("constructor_standings"):
                    self.constructor_standings = raw["constructor_standings"]
                if raw.get("latest_race"):
                    self.latest_race = raw["latest_race"]
                if raw.get("season_calendar"):
                    self.season_calendar = raw["season_calendar"]
                for ck, qr in raw.get("quali_results", {}).items():
                    if ck not in self.quali_results:
                        self.quali_results[ck] = qr
                for ck, qt in raw.get("quali_times", {}).items():
                    if ck not in self.quali_times:
                        self.quali_times[ck] = qt
                self.sprint_quali_results = raw.get("sprint_quali_results", {})
                self.sprint_quali_times = raw.get("sprint_quali_times", {})
                # Merge session completion from raw fallback
                for ck, types in raw.get("session_completion", {}).items():
                    if ck not in self.session_completion:
                        self.session_completion[ck] = types
                    else:
                        self.session_completion[ck] |= types
                # Merge live FP times from raw fallback
                for ck, fp_data in raw.get("live_fp_times", {}).items():
                    if ck not in self.live_fp_times:
                        self.live_fp_times[ck] = fp_data
                if raw.get("calibration_races", 0) > 0:
                    with _GC_LOCK:
                        GLOBAL_CORRECTION = raw["calibration_correction"]
                    self.calibration_races = raw["calibration_races"]
            except Exception as exc:
                self.after(0, self._show_status, f"Raw data error: {exc}")

        # Step 4: Cross-reference with formula1-datasets CSV (supplementary)
        try:
            cb = lambda msg: self.after(0, self._show_status, msg)
            csv_quali, csv_qtimes = fetch_csv_qualifying(
                progress_callback=cb)
            for ck, qr in csv_quali.items():
                if ck not in self.quali_results:
                    self.quali_results[ck] = qr
            for ck, qt in csv_qtimes.items():
                if ck not in self.quali_times:
                    self.quali_times[ck] = qt

            csv_sq, csv_sqt = fetch_csv_sprint_qualifying(
                progress_callback=cb)
            for ck, sqr in csv_sq.items():
                if ck not in self.sprint_quali_results:
                    self.sprint_quali_results[ck] = sqr
            for ck, sqt in csv_sqt.items():
                if ck not in self.sprint_quali_times:
                    self.sprint_quali_times[ck] = sqt

            _, csv_standings = fetch_csv_race_results(
                progress_callback=cb)
            if not self.driver_standings and csv_standings:
                self.driver_standings = csv_standings

            csv_sprint, _ = fetch_csv_sprint_results(
                progress_callback=cb)
            for ck in csv_sprint:
                self.session_completion.setdefault(ck, set()).add(
                    "sprint-race")
        except Exception as exc:
            self.after(0, self._show_status, f"CSV cross-reference error: {exc}")

        self.after(0, self._on_all_data_loaded)

    def _on_historical_loaded(self):
        """Main-thread callback: enable selector after historical data loads."""
        self.circuit_combo.configure(state="readonly")
        if self.circuit_combo.current() < 0:
            self.circuit_combo.current(0)
        self.on_circuit_selected()
        self._update_championship_projection()
        if self.historical_data:
            self._show_status("Loading latest season data...")
        else:
            self._show_status("Historical data unavailable")

    def _on_all_data_loaded(self):
        """Main-thread callback: update all GUI panels after f1db data loads."""
        self._update_standings()
        self._update_latest_race()
        self._update_circuit_selector()
        # Refresh session status for currently selected circuit
        if self.circuit_combo.current() >= 0:
            self._update_session_status(self._get_selected_circuit_key())
        self._update_calibration()
        self._update_championship_projection()
        self._show_status("")
        self.refresh_btn.configure(state="normal")

    def _refresh_data(self):
        """Re-fetch all data (f1nsight + f1db) in background without restarting."""
        self.refresh_btn.configure(state="disabled")
        self.circuit_combo.configure(state="disabled")
        self._show_status("Refreshing data...")
        threading.Thread(target=self._load_all_data, daemon=True).start()

    def _show_status(self, text: str):
        if hasattr(self, "status_label"):
            self.status_label.configure(text=text)

    def _toggle_section(self, section_name, content_frame, toggle_btn):
        """Toggle visibility of a collapsible section."""
        collapsed = self._collapsed.get(section_name, False)
        if collapsed:
            content_frame.grid()
            toggle_btn.configure(text="\u25BC")
            self._collapsed[section_name] = False
        else:
            content_frame.grid_remove()
            toggle_btn.configure(text="\u25B6")
            self._collapsed[section_name] = True

    def _build_standings(self):
        # Header bar with toggle button
        header_frame = ttk.Frame(self, padding=(20, 5, 20, 0))
        header_frame.grid(row=4, column=0, sticky="ew")
        self._standings_toggle = tk.Button(
            header_frame, text="\u25BC", command=lambda: self._toggle_section(
                "standings", self._standings_content, self._standings_toggle),
            bg="#15151E", fg="#E10600", bd=0, font=("Segoe UI", 10, "bold"),
            activebackground="#15151E", activeforeground="#FFFFFF", cursor="hand2")
        self._standings_toggle.pack(side="left")
        ttk.Label(header_frame, text=" Current Standings", style="Sub.TLabel",
                  font=("Segoe UI", 11, "bold"), foreground="#E10600").pack(side="left")

        # Collapsible content
        self._standings_content = ttk.Frame(self, padding=(20, 0, 20, 10))
        self._standings_content.grid(row=5, column=0, sticky="ew")
        self._standings_content.columnconfigure(0, weight=1)
        self._standings_content.columnconfigure(1, weight=1)

        # Driver standings
        self.driver_standings_frame = ttk.LabelFrame(self._standings_content, text="Driver Championship",
                                                      padding=(10, 5))
        self.driver_standings_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))

        self.driver_standings_labels = []
        self.driver_no_data = ttk.Label(self.driver_standings_frame,
                                         text="No races completed \u2014 standings available after Round 1",
                                         style="Sub.TLabel")
        self.driver_no_data.pack(anchor="w")

        # Constructor standings
        self.constructor_standings_frame = ttk.LabelFrame(self._standings_content, text="Constructor Championship",
                                                           padding=(10, 5))
        self.constructor_standings_frame.grid(row=0, column=1, sticky="nsew", padx=(5, 0))

        self.constructor_standings_labels = []
        self.constructor_no_data = ttk.Label(self.constructor_standings_frame,
                                              text="No races completed \u2014 standings available after Round 1",
                                              style="Sub.TLabel")
        self.constructor_no_data.pack(anchor="w")

    def _build_championship_projection(self):
        """Build the projected season championship panel."""
        # Header bar with toggle button
        champ_header = ttk.Frame(self, padding=(20, 5, 20, 0))
        champ_header.grid(row=6, column=0, sticky="ew")
        self._champ_toggle = tk.Button(
            champ_header, text="\u25BC", command=lambda: self._toggle_section(
                "championship", self._champ_content, self._champ_toggle),
            bg="#15151E", fg="#E10600", bd=0, font=("Segoe UI", 10, "bold"),
            activebackground="#15151E", activeforeground="#FFFFFF", cursor="hand2")
        self._champ_toggle.pack(side="left")
        ttk.Label(champ_header, text=" Projected Season Championship", style="Sub.TLabel",
                  font=("Segoe UI", 11, "bold"), foreground="#E10600").pack(side="left")

        # Collapsible content
        self._champ_content = ttk.LabelFrame(self, text="E[Pts] across all circuits",
                                              padding=(10, 5))
        self._champ_content.grid(row=7, column=0, sticky="ew", padx=20, pady=(0, 10))
        self._champ_content.columnconfigure(0, weight=1)
        frame = self._champ_content

        self.champ_tree = ttk.Treeview(
            frame,
            columns=("pos", "driver", "team", "actual", "projected", "total", "races"),
            show="headings", selectmode="browse", height=10
        )

        champ_cols = {
            "pos":       ("Pos",    40,  "center"),
            "driver":    ("Driver", 160, "w"),
            "team":      ("Team",   150, "w"),
            "actual":    ("Current Pts", 85,  "center"),
            "projected": ("Remaining", 80,  "center"),
            "total":     ("Est. Final", 80,  "center"),
            "races":     ("Races Projected", 110, "center"),
        }
        for cid, (heading, width, anchor) in champ_cols.items():
            self.champ_tree.heading(cid, text=heading)
            self.champ_tree.column(cid, width=width, anchor=anchor, minwidth=40)

        # Apply team color tags
        for tag, colors in TEAM_COLORS.items():
            self.champ_tree.tag_configure(tag, background=colors["bg"], foreground=colors["fg"])

        self.champ_tree.pack(fill="x", expand=True)

        self.champ_status = ttk.Label(frame, text="Computing season projections...",
                                       style="Sub.TLabel", font=("Segoe UI", 9, "italic"))
        self.champ_status.pack(anchor="w", pady=(2, 0))

    def _update_championship_projection(self):
        """Recalculate and display projected season championship."""
        for item in self.champ_tree.get_children():
            self.champ_tree.delete(item)

        projections = calculate_season_projection(
            self.historical_data,
            self.driver_standings if self.driver_standings else None,
            self.season_calendar if self.season_calendar else None,
            self.live_fp_times if self.live_fp_times else None,
        )

        completed = sum(1 for e in self.season_calendar if e.get("completed")) if self.season_calendar else 0
        total_races = len(CIRCUITS_2026)
        remaining = total_races - completed

        for i, p in enumerate(projections, start=1):
            self.champ_tree.insert("", "end",
                                    values=(i, p["driver"], p["team"],
                                            f"{p['actual_pts']:.0f}" if p['actual_pts'] > 0 else "\u2014",
                                            f"{p['projected_pts']:.1f}",
                                            f"{p['total_pts']:.1f}",
                                            p["races_projected"]),
                                    tags=(p["tag"],))

        status = f"{completed}/{total_races} races completed \u00b7 {remaining} projected"
        if completed > 0:
            status += f" \u00b7 Using actual points for {completed} race{'s' if completed != 1 else ''}"
        status += "  |  Current Pts = official standings \u00b7 Remaining = projected future races \u00b7 Est. Final = Current + Remaining"
        self.champ_status.configure(text=status)

    def _update_standings(self):
        """Refresh standings display from self.driver_standings / self.constructor_standings."""
        # Clear old labels
        for lbl in self.driver_standings_labels:
            lbl.destroy()
        self.driver_standings_labels.clear()
        for lbl in self.constructor_standings_labels:
            lbl.destroy()
        self.constructor_standings_labels.clear()

        if self.driver_standings:
            self.driver_no_data.pack_forget()
            for s in self.driver_standings[:10]:
                text = f"P{s['position']}  {s['driver']}  \u2014  {s['points']:.0f} pts"
                lbl = ttk.Label(self.driver_standings_frame, text=text,
                                font=("Segoe UI", 10), style="Sub.TLabel")
                lbl.pack(anchor="w", pady=1)
                self.driver_standings_labels.append(lbl)
        else:
            self.driver_no_data.pack(anchor="w")

        if self.constructor_standings:
            self.constructor_no_data.pack_forget()
            for s in self.constructor_standings[:5]:
                cname = s['constructor_id'].replace('-', ' ').title()
                text = f"P{s['position']}  {cname}  \u2014  {s['points']:.0f} pts"
                lbl = ttk.Label(self.constructor_standings_frame, text=text,
                                font=("Segoe UI", 10), style="Sub.TLabel")
                lbl.pack(anchor="w", pady=1)
                self.constructor_standings_labels.append(lbl)
        else:
            self.constructor_no_data.pack(anchor="w")

    def _update_latest_race(self):
        """Refresh the latest race summary display."""
        if self.latest_race is None:
            self.latest_race_label.configure(text="Season starts March 16 \u2014 Melbourne")
            return

        race = self.latest_race
        gp = race["grand_prix"].replace("-", " ").title()
        top5 = race["results"][:5]
        parts = [f"R{race['round']} {gp} ({race['date']}):"]
        for r in top5:
            fl = " \U0001f7e3" if r["fastest_lap"] else ""
            parts.append(f"P{r['position']} {r['driver']}{fl}")
        self.latest_race_label.configure(text="  \u00b7  ".join(parts))

    def _update_circuit_selector(self):
        """Rebuild circuit selector with round numbers and completion status."""
        if not self.season_calendar:
            return  # Keep default names

        # Build mapping from f1db circuit_id to calendar entry
        calendar_by_circuit = {}
        for entry in self.season_calendar:
            calendar_by_circuit[entry["circuit_id"]] = entry

        enhanced_names = []
        for circuit_key, circuit in CIRCUITS_2026.items():
            f1db_id = CIRCUIT_F1DB_IDS.get(circuit_key, "")
            cal = calendar_by_circuit.get(f1db_id)
            if cal:
                marker = "\u2713" if cal["completed"] else "\u00b7"
                prefix = f"R{cal['round']:02d} {marker} "
            else:
                prefix = "     "
            enhanced_names.append(f"{prefix}{circuit['name']}")

        current_idx = self.circuit_combo.current()
        self.circuit_combo.configure(values=enhanced_names)
        if 0 <= current_idx < len(enhanced_names):
            self.circuit_combo.current(current_idx)

    def _update_calibration(self):
        """Update calibration status display."""
        if self.calibration_races == 0:
            self.calibration_label.configure(text="Calibration: baseline (no races)")
        else:
            self.calibration_label.configure(
                text=f"Calibration: {GLOBAL_CORRECTION:.4f} ({self.calibration_races} race{'s' if self.calibration_races != 1 else ''})"
            )

    def _update_session_status(self, circuit_key):
        """Update the session status label for the selected circuit."""
        completed = self.session_completion.get(circuit_key, set())
        is_sprint = circuit_key in SPRINT_CIRCUITS
        if is_sprint:
            sessions = ["FP1", "SQ", "Sprint", "Q", "Race"]
        else:
            sessions = ["FP1", "FP2", "FP3", "Q", "Race"]
        parts = []
        for label in sessions:
            db_type = SESSION_TYPE_MAP[label]
            marker = "\u2713" if db_type in completed else "\u00b7"
            parts.append(f"{label} {marker}")
        self.session_status_label.configure(text="  ".join(parts))

    def on_circuit_selected(self, event=None):
        circuit_key = self._get_selected_circuit_key()
        self._update_session_status(circuit_key)
        circuit = CIRCUITS_2026[circuit_key]
        mode = self.mode_combo.get()  # "Grand Prix", "Grand Prix +Q", or "Sprint"

        # Update info label
        overtaking = CIRCUIT_OVERTAKING.get(circuit_key, 0.50)
        ot_label = ("Very Hard" if overtaking <= 0.15 else
                    "Hard" if overtaking <= 0.35 else
                    "Medium" if overtaking <= 0.55 else
                    "Easy" if overtaking <= 0.75 else "Very Easy")

        quali = self.quali_results.get(circuit_key, {})
        quali_times = self.quali_times.get(circuit_key, {})
        live_fp = self.live_fp_times.get(circuit_key)

        if mode == "Sprint":
            if circuit_key in SPRINT_CIRCUITS:
                sq_pos = self.sprint_quali_results.get(circuit_key, {})
                sq_times_data = self.sprint_quali_times.get(circuit_key, {})
                projections = calculate_sprint_projections(
                    circuit_key, sq_pos, sq_times_data, self.historical_data, live_fp)
                sprint_info = SPRINT_CIRCUITS[circuit_key]
                mode_text = f"Sprint ({sprint_info['sprint_laps']} laps) · SQ + FP1"
                if not sq_pos:
                    mode_text += " (no sprint qualifying data yet)"
            else:
                projections = calculate_all_projections(
                    circuit_key, self.historical_data, quali or None, live_fp)
                mode_text = "No sprint race at this circuit — showing Grand Prix projection"
        elif mode == "Grand Prix +Q" and quali:
            projections = calculate_qualifying_projections(
                circuit_key, quali, quali_times, self.historical_data, live_fp)
            mode_text = "Grand Prix +Q (FP1/2/3 + Qualifying) + Historical"
        else:
            projections = calculate_all_projections(
                circuit_key, self.historical_data, quali or None, live_fp)
            mode_text = "Grand Prix (FP1/2/3) + Historical"
            if mode == "Grand Prix +Q" and not quali:
                mode_text += " (no qualifying data yet)"
        if live_fp:
            mode_text += " · Live FP"

        self.circuit_info.configure(
            text=f"{circuit['location']} · {circuit['km']} km · {circuit['turns']} turns · {circuit['laps']} laps · {circuit['type'].title()} · Overtaking: {ot_label} ({overtaking:.0%}) · {mode_text}"
        )

        # Clear and repopulate table
        if mode == "Sprint" and circuit_key in SPRINT_CIRCUITS:
            projections = projections[:10]
        for item in self.tree.get_children():
            self.tree.delete(item)

        for i, p in enumerate(projections, start=1):
            display_name = p["driver"] + ("*" if p["data_quality"] == "estimated" else "")
            quali_display = f"P{p['quali_pos']}" if p.get("quali_pos") is not None else "—"
            pos_low = p.get("pos_low")
            pos_high = p.get("pos_high")
            if pos_low is not None and pos_high is not None:
                if pos_low == pos_high:
                    range_str = f"P{pos_low}"
                else:
                    range_str = f"P{pos_low}-P{pos_high}"
            else:
                range_str = "—"
            self.tree.insert("", "end",
                             values=(i, display_name, p["team"], quali_display,
                                     p["time_str"], p["gap_str"],
                                     p.get("dnf_pct", "—"), p.get("proj_pts", 0),
                                     p.get("exp_pts", "—"), range_str),
                             tags=(p["tag"],))

    def _build_footer(self):
        frame = ttk.Frame(self, padding=(20, 0, 20, 10))
        frame.grid(row=3, column=0, sticky="ew")
        self.footer_label = ttk.Label(
            frame,
            text="* Estimated baseline  ·  DNF% = retirement probability (team + driver history)  ·  E[Pts] = expected points (accounts for DNF risk + overtaking difficulty)  ·  Range = 80% confidence interval for finishing position",
            style="Sub.TLabel",
            font=("Segoe UI", 9, "italic"),
        )
        self.footer_label.pack(anchor="w")

    def _sort_by_column(self, col):
        data = [(self.tree.set(item, col), item) for item in self.tree.get_children("")]
        try:
            data.sort(key=lambda t: float(
                t[0].replace("+", "").replace("s", "")
                    .replace("LEADER", "0").replace("\u2014", "999")
                    .replace("P", "").split("-")[0]
            ))
        except ValueError:
            data.sort(key=lambda t: t[0])
        for index, (_, item) in enumerate(data):
            self.tree.move(item, "", index)


if __name__ == "__main__":
    app = F1ProjectionApp()
    app.mainloop()
