"""Funções utilitárias e conversões temporais para maratonas."""

from typing import Optional, Union
import numpy as np
import pandas as pd


def parse_time_string_to_seconds(time_series: pd.Series) -> pd.Series:
    """Converte uma Série Pandas de strings de tempo ('HH:MM:SS' ou 'H:MM:SS') em segundos contínuos (float64).

    Parameters
    ----------
    time_series : pd.Series
        Série contendo representações em string dos tempos.

    Returns
    -------
    pd.Series
        Série em segundos contínuos (float), com nulos tipados como np.nan.
    """

    def _convert_single(val: Union[str, float, None]) -> float:
        if pd.isna(val) or val in ("", "-", None):
            return np.nan
        try:
            parts = str(val).strip().split(":")
            if len(parts) == 3:
                return (
                    float(parts[0]) * 3600
                    + float(parts[1]) * 60
                    + float(parts[2])
                )
            elif len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            return np.nan
        except (ValueError, TypeError):
            return np.nan

    return time_series.apply(_convert_single)


def time_to_seconds(val: Union[str, float, None]) -> float:
    """Converte uma única string 'HH:MM:SS' para segundos contínuos."""
    if pd.isna(val) or val in ("", "-", None):
        return np.nan
    try:
        parts = str(val).strip().split(":")
        if len(parts) == 3:
            return (
                float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            )
        elif len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        return np.nan
    except (ValueError, TypeError):
        return np.nan


def format_seconds_to_pace(seconds_per_km: Optional[float]) -> str:
    """Converte ritmo numérico (segundos/km) para string formatada 'MM:SS/km'."""
    if pd.isna(seconds_per_km) or seconds_per_km <= 0:
        return "-"
    mins = int(seconds_per_km // 60)
    secs = int(seconds_per_km % 60)
    return f"{mins:02d}:{secs:02d}/km"