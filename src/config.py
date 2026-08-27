"""Módulo de configuração central do projeto."""

from pathlib import Path
from typing import Dict, List

# Diretórios de persistência
BASE_DIR: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = BASE_DIR / "data"
RAW_DATA_DIR: Path = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"

# Parâmetros de conexão com API Supabase (B.A.A.)
BASE_URL: str = "https://uhtvvsvutviasndmyemn.supabase.co/rest/v1/race_results"
API_KEY: str = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVodHZ2c3Z1dHZpYXNuZG15ZW1uIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkxNTA5NTEsImV4cCI6MjA2NDcyNjk1MX0."
    "7J4yfKKrGTNTGyZlsjP9TXj1r3JEvZ8tRWT1CJpYVXk"
)

DEFAULT_HEADERS: Dict[str, str] = {
    "apikey": API_KEY,
    "authorization": f"Bearer {API_KEY}",
    "accept-profile": "public",
    "prefer": "count=exact",  # <-- Força o Supabase a devolver a contagem exata
    "Origin": "https://www.baa.org",
    "Referer": "https://www.baa.org/",
}

# Schema canônico das variáveis brutas
RAW_COLUMNS: List[str] = [
    "ID",
    "EventYear",
    "FullBibNumber",
    "FormattedFullName",
    "AgeOnRaceDay",
    "GenderCode",
    "AwardsDivisionShortDesc",
    "CountryOfResidenceName",
    "CountryOfCTZName",
    "City",
    "StateName",
    "Time5K",
    "Time10K",
    "Time15K",
    "Time20K",
    "TimeHalf",
    "Time25K",
    "Time30K",
    "Time35K",
    "Time40K",
    "ChipFinish",
    "RankOverAll",
    "RankOverGender",
    "RankOverDivision",
]

SPLIT_COLUMNS: List[str] = [
    "Time5K",
    "Time10K",
    "Time15K",
    "Time20K",
    "TimeHalf",
    "Time25K",
    "Time30K",
    "Time35K",
    "Time40K",
    "ChipFinish",
]

MARATHON_DISTANCE_KM: float = 42.195
FINAL_SEGMENT_KM: float = 2.195
HIT_THE_WALL_THRESHOLD: float = 1.20
MIN_PLAUSIBLE_PACE_MIN_PER_KM: float = 2.50
EXTREME_NEGATIVE_SPLIT_RATIO: float = 0.75