"""Módulo de transformação, engenharia de atributos fisiológicos e auditoria de integridade.

Aplica padronização canônica, conversão temporal vetorial, separação entre ausência
histórica de infraestrutura e detecção de fraude, cálculo de dinâmicas de pacing e métricas de quebra.
"""

import logging
from typing import List
import numpy as np
import pandas as pd

from src import config
from src.utils import parse_time_string_to_seconds

logger = logging.getLogger(__name__)


class MarathonTransformer:
    """Pipeline de processamento analítico para as World Marathon Majors."""

    def __init__(self):
        pass

    @staticmethod
    def _convert_split_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Converte todas as colunas de tempo brutas em segundos contínuos (float64)."""
        for col in config.SPLIT_COLUMNS:
            if col in df.columns:
                df[f"{col}_sec"] = parse_time_string_to_seconds(df[col])
            else:
                df[f"{col}_sec"] = np.nan
        return df

    @staticmethod
    def _evaluate_edition_infrastructure(df: pd.DataFrame) -> pd.DataFrame:
        """Identifica se cada edição anual possuía infraestrutura digital de tapetes de 5 km ativa."""
        # Uma edição é considerada como tendo infraestrutura se > 50% dos concluintes registraram Time5K e TimeHalf
        splits_coverage = (
            df.groupby("EventYear")
            .apply(
                lambda g: (
                    g["Time5K_sec"].notnull() & g["TimeHalf_sec"].notnull()
                ).mean()
                >= 0.50
            )
            .to_dict()
        )

        df["Edition_Has_Splits"] = df["EventYear"].map(splits_coverage)

        # Flag indicando se o atleta individual possui todos os 10 tapetes para modelagem de pacing
        canonical_mats = [f"{col}_sec" for col in config.SPLIT_COLUMNS]
        df["Is_Pacing_Analysable"] = df[canonical_mats].notnull().all(axis=1)

        return df

    @staticmethod
    def _calculate_segment_deltas(df: pd.DataFrame) -> pd.DataFrame:
        """Calcula o tempo líquido decorrido em cada intervalo intermediário (em segundos)."""
        df["Delta_0_5k_sec"] = df["Time5K_sec"]
        df["Delta_5_10k_sec"] = df["Time10K_sec"] - df["Time5K_sec"]
        df["Delta_10_15k_sec"] = df["Time15K_sec"] - df["Time10K_sec"]
        df["Delta_15_20k_sec"] = df["Time20K_sec"] - df["Time15K_sec"]
        df["Delta_20_25k_sec"] = df["Time25K_sec"] - df["Time20K_sec"]
        df["Delta_25_30k_sec"] = df["Time30K_sec"] - df["Time25K_sec"]
        df["Delta_30_35k_sec"] = df["Time35K_sec"] - df["Time30K_sec"]
        df["Delta_35_40k_sec"] = df["Time40K_sec"] - df["Time35K_sec"]
        df["Delta_40_End_sec"] = df["ChipFinish_sec"] - df["Time40K_sec"]
        return df

    @staticmethod
    def _calculate_segment_paces(df: pd.DataFrame) -> pd.DataFrame:
        """Calcula o ritmo médio de cada segmento em minutos por quilômetro (min/km)."""
        split_segments = [
            ("0_5k", "Delta_0_5k_sec", 5.0),
            ("5_10k", "Delta_5_10k_sec", 5.0),
            ("10_15k", "Delta_10_15k_sec", 5.0),
            ("15_20k", "Delta_15_20k_sec", 5.0),
            ("20_25k", "Delta_20_25k_sec", 5.0),
            ("25_30k", "Delta_25_30k_sec", 5.0),
            ("30_35k", "Delta_30_35k_sec", 5.0),
            ("35_40k", "Delta_35_40k_sec", 5.0),
            ("40_End", "Delta_40_End_sec", config.FINAL_SEGMENT_KM),
        ]

        for suffix, delta_col, distance in split_segments:
            df[f"Pace_{suffix}_min_per_km"] = (df[delta_col] / distance) / 60.0

        df["Pace_Overall_min_per_km"] = (
            df["ChipFinish_sec"] / config.MARATHON_DISTANCE_KM
        ) / 60.0

        return df

    @staticmethod
    def _compute_physiological_metrics(
        df: pd.DataFrame, race_name: str
    ) -> pd.DataFrame:
        """Calcula métricas fisiológicas de fadiga, razão de pacing e quebra."""
        df["Half1_sec"] = df["TimeHalf_sec"]
        df["Half2_sec"] = df["ChipFinish_sec"] - df["TimeHalf_sec"]

        # Pacing Ratio: Razão entre a 2ª metade e a 1ª metade
        df["Pacing_Ratio"] = df["Half2_sec"] / df["Half1_sec"]

        # Hit the Wall (HTW): Segunda metade >= 20% mais lenta
        df["Hit_The_Wall"] = df["Pacing_Ratio"] >= config.HIT_THE_WALL_THRESHOLD

        # Queda específica de ritmo nas colinas de Newton (Boston)
        if race_name.lower() == "boston":
            pace_first_half = (df["Time20K_sec"] / 20.0) / 60.0
            df["Newton_Hills_Decay_Pct"] = (
                (df["Pace_30_35k_min_per_km"] - pace_first_half)
                / pace_first_half
            ) * 100.0
        else:
            df["Newton_Hills_Decay_Pct"] = np.nan

        return df

    @staticmethod
    def _audit_data_integrity(df: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de integridade ética distinguindo ausência tecnológica de fraude."""
        canonical_intermediate_mats = [
            f"{col}_sec" for col in config.SPLIT_COLUMNS[:-1]
        ]

        # Flag 1 (Auditoria Ética REAL): O atleta perdeu tapetes intermediários APENAS em edições com infraestrutura ativa!
        df["Flag_Missing_Mats"] = (
            df["Edition_Has_Splits"]
            & df[canonical_intermediate_mats].isnull().any(axis=1)
            & df["ChipFinish_sec"].notnull()
        )

        pace_columns = [
            "Pace_0_5k_min_per_km",
            "Pace_5_10k_min_per_km",
            "Pace_10_15k_min_per_km",
            "Pace_15_20k_min_per_km",
            "Pace_20_25k_min_per_km",
            "Pace_25_30k_min_per_km",
            "Pace_30_35k_min_per_km",
            "Pace_35_40k_min_per_km",
        ]

        # Flag 2: Ritmo fisicamente impossível (< 2:30 min/km ou valores negativos)
        df["Flag_Pace_Impossivel"] = (
            (df[pace_columns] < config.MIN_PLAUSIBLE_PACE_MIN_PER_KM)
            | (df[pace_columns] <= 0.0)
        ).any(axis=1)

        # Flag 3: Negative split extremo improvável (2ª metade 25% mais rápida que a 1ª)
        df["Flag_Negative_Split_Extremo"] = (
            df["Pacing_Ratio"] < config.EXTREME_NEGATIVE_SPLIT_RATIO
        )

        return df

    def transform(
        self, df_raw: pd.DataFrame, race_name: str = "Boston"
    ) -> pd.DataFrame:
        """Executa a esteira analítica preservando 100% dos concluintes com flags metodológicas."""
        if df_raw.empty:
            logger.warning("Transformação abortada: DataFrame de entrada vazio.")
            return pd.DataFrame()

        logger.info(
            f"Transformando base de dados de {race_name}: {len(df_raw):,} registros brutos."
        )

        df = df_raw.copy()
        df["Race"] = race_name.capitalize()

        # 1. Conversão temporal
        df = self._convert_split_columns(df)

        # 2. Avaliação de infraestrutura da edição (Separa tecnologia de fraude)
        df = self._evaluate_edition_infrastructure(df)

        # 3. Engenharia de atributos fisiológicos
        df = self._calculate_segment_deltas(df)
        df = self._calculate_segment_paces(df)
        df = self._compute_physiological_metrics(df, race_name)

        # 4. Auditoria ética
        df = self._audit_data_integrity(df)

        logger.info(
            f"Transformação concluída: {len(df):,} atletas preservados na base processada. "
            f"Dimensões finais: {df.shape[0]:,} linhas x {df.shape[1]} colunas."
        )

        return df


# Alias de compatibilidade
BostonMarathonTransformer = MarathonTransformer