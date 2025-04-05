import logging
from datetime import date, datetime, timedelta
from math import isnan
from typing import List, Optional

from pydantic import BaseModel, field_validator, model_validator

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Aoedumps schema
# -----------------------------------------------------------------------------


class WeeklyDump(BaseModel):
    start_date: date
    end_date: Optional[date] = None
    num_matches: int
    num_players: Optional[int] = None
    matches_url: str
    players_url: str
    match_checksum: Optional[str] = None
    player_checksum: Optional[str] = None

    @model_validator(mode="after")
    def validate_and_get_message(self) -> "WeeklyDump":
        if self.num_matches == 0:
            msg = f"File {self.start_date} is present but is empty!"
            logger.warning(msg)
        return self


class ApiSchema(BaseModel):
    db_dumps: List[WeeklyDump]
    total_matches: Optional[int] = None
    total_players: Optional[int] = None


# -----------------------------------------------------------------------------
# RelicAPI schema
# -----------------------------------------------------------------------------


class RelicResult(BaseModel):
    code: int
    message: str


class Member(BaseModel):
    profile_id: Optional[int | float] = None
    name: Optional[str] = None
    alias: str
    personal_statgroup_id: Optional[int] = None
    xp: Optional[int] = None
    level: Optional[int] = None
    leaderboardregion_id: Optional[int] = None
    country: str


class StatGroup(BaseModel):
    id: int
    name: Optional[str] = None
    type: Optional[int] = None
    members: List[Member]


class LeaderboardStat(BaseModel):
    statgroup_id: int
    leaderboard_id: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    streak: Optional[int] = None
    disputes: Optional[int] = None
    drops: Optional[int] = None
    rank: int
    ranktotal: Optional[int] = None
    ranklevel: Optional[int] = None
    rating: Optional[int] = None
    regionrank: Optional[int] = None
    regionranktotal: Optional[int] = None
    lastmatchdate: int
    highestrank: Optional[int] = None
    highestranklevel: Optional[int] = None
    highestrating: Optional[int] = None


class RelicResponse(BaseModel):
    statGroups: List[StatGroup]
    leaderboardStats: List[LeaderboardStat]


# -------------------------------Aoestats .parquet schemas


class Players(BaseModel):
    civ: str
    game_id: int
    match_rating_diff: float
    new_rating: int
    old_rating: int
    profile_id: Optional[int] = None
    replay_summary_raw: Optional[str] = None
    team: int
    winner: bool

    @field_validator('profile_id', mode='before')
    def cast_profile_id_to_int(cls, v):
        if v is None or (isinstance(v, float) and isnan(v)):
            return None
        try:
            return int(float(v))
        except ValueError as e:
            raise ValueError(f"Cannot convert profile_id '{v}' to int: {e}")

    @field_validator('replay_summary_raw', mode='before')
    def cast_replay_summary_raw_to_str(cls, v):
        if v is None:
            return None
        return str(v)


class Matches(BaseModel):
    avg_elo: float
    duration: timedelta
    game_id: int
    game_speed: str
    game_type: str
    irl_duration: timedelta
    leaderboard: str
    map: str
    mirror: bool
    num_players: int
    patch: int
    raw_match_type: Optional[float] = None
    replay_enhanced: bool
    started_timestamp: datetime
    starting_age: str
    team_0_elo: float
    team_1_elo: float

    @field_validator('raw_match_type', mode='before')
    def cast_raw_match_type_to_float(cls, v):
        if v is None:
            return None
        try:
            return float(v)
        except ValueError as e:
            raise ValueError(f"Cannot convert raw_match_type '{v}' to float: {e}")
