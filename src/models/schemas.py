from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AthleteEvent:
    athlete_id: int
    name: str
    sex: str
    age: Optional[float]
    height: Optional[float]
    weight: Optional[float]
    team: str
    noc: str
    games: str
    year: int
    season: str
    city: str
    sport: str
    event: str
    medal: Optional[str]


@dataclass(frozen=True)
class NocRegion:
    noc: str
    region: Optional[str]
    notes: Optional[str]


ATHLETE_TRACKED_COLS: list[str] = ["name", "sex", "team"]
NOC_TRACKED_COLS: list[str]     = ["region", "notes"]
EVENT_TRACKED_COLS: list[str]   = ["sport", "season"]
GAME_COLS: list[str]            = ["games", "year", "season", "city"]
