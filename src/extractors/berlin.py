"""Módulo de extração e padronização de dados da Maratona de Berlim (SCC Events).

Implementa estratégias de paginação dinâmica, resolução de identificadores
históricos e persistência raw integral.
"""

import logging
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src import config

logger = logging.getLogger(__name__)

BERLIN_API_URL: str = "https://api.results.scc-events.com/result"

BERLIN_HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://results.scc-events.com",
    "Referer": "https://results.scc-events.com/",
}


class BerlinMarathonExtractor:
    """Cliente HTTP para extração completa e padronização dos resultados de Berlim."""

    def __init__(self, page_size: int = 100):
        self.page_size = page_size
        self.session = self._build_retry_session()

    @staticmethod
    def _build_retry_session() -> requests.Session:
        """Configura a sessão HTTP com política de retry para erros transientes de rede."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _resolve_year_params(self, year: int) -> Tuple[Optional[str], int]:
        """Identifica dinamicamente o identificador de percurso (ci) e o total de atletas.

        Parameters
        ----------
        year : int
            Ano da edição.

        Returns
        -------
        Tuple[Optional[str], int]
            Tupla contendo o código 'ci' válido ('BML' ou 'MAL') e a contagem total de concluintes.
        """
        # Edição de 2020 cancelada devido à pandemia
        if year == 2020:
            return None, 0

        # Ordem de tentativa conforme histórico da SCC Events
        candidate_ci = ["BML", "MAL"] if year >= 2021 or year <= 2004 else ["MAL", "BML"]

        for ci in candidate_ci:
            params = {
                "ek": "BM",
                "ci": ci,
                "y": str(year),
                "t": f"BM_{year}",
                "draw": "1",
                "start": "0",
                "length": "1",
            }
            try:
                resp = self.session.get(
                    BERLIN_API_URL,
                    headers=BERLIN_HEADERS,
                    params=params,
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    total = data.get("recordsTotal", 0)
                    if total and total > 0:
                        return ci, int(total)
            except Exception:
                continue

        return None, 0

    def extract_year(
        self, year: int, destination_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """Extrai todos os maratonistas de uma edição com paginação contínua e sem perdas.

        Parameters
        ----------
        year : int
            Ano a ser extraído.
        destination_dir : Optional[Path]
            Diretório para persistência do arquivo bruto intermediário.

        Returns
        -------
        pd.DataFrame
            DataFrame padronizado no schema canônico do projeto.
        """
        ci_key, total_participants = self._resolve_year_params(year)

        if not ci_key or total_participants == 0:
            logger.warning(
                f"Ano {year}: Nenhum participante localizado na API de Berlim."
            )
            return pd.DataFrame()

        logger.info(
            f"Extraindo Berlin Marathon {year} (ci='{ci_key}', Total: {total_participants:,} atletas)"
        )

        raw_records: List[Dict] = []
        offset: int = 0
        draw: int = 1

        with tqdm(
            total=total_participants, desc=f"Berlim {year}", unit=" atletas"
        ) as pbar:
            while True:
                params = {
                    "ek": "BM",
                    "ci": ci_key,
                    "y": str(year),
                    "t": f"BM_{year}",
                    "draw": str(draw),
                    "start": str(offset),
                    "length": str(self.page_size),
                    "order[0][column]": "0",
                    "order[0][dir]": "asc",
                }

                resp = self.session.get(
                    BERLIN_API_URL,
                    headers=BERLIN_HEADERS,
                    params=params,
                    timeout=25,
                )
                resp.raise_for_status()
                batch_data = resp.json().get("data", [])

                # Condição de parada definitiva: API esgotou os registros
                if not batch_data:
                    break

                raw_records.extend(batch_data)
                pbar.update(len(batch_data))

                # Incrementa o offset exatamente pelo volume real retornado pelo servidor
                offset += len(batch_data)
                draw += 1
                time.sleep(0.02)

        if not raw_records:
            logger.warning(f"Ano {year}: Nenhum registro recuperado.")
            return pd.DataFrame()

        df_raw = pd.DataFrame(raw_records)

        # -------------------------------------------------------------
        # 1. Persistência do Raw Lake Integral (Todas as colunas do JSON)
        # -------------------------------------------------------------
        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"berlin_{year}_raw.parquet"
            df_raw.to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} persistido (raw completo): {len(df_raw):,} atletas salvos em {output_file.name}"
            )

        # -------------------------------------------------------------
        # 2. Padronização para o Schema Canônico Global
        # -------------------------------------------------------------
        df_standard = pd.DataFrame()
        df_standard["ID"] = df_raw.get("id")
        df_standard["EventYear"] = year
        df_standard["FullBibNumber"] = df_raw.get("startnummer", "").astype(str)
        df_standard["FormattedFullName"] = df_raw.get("name")

        # Tratamento de Gênero (W -> F)
        if "sex" in df_raw.columns:
            df_standard["GenderCode"] = df_raw["sex"].replace(
                {"W": "F", "w": "F", "M": "M", "m": "M"}
            )
        else:
            df_standard["GenderCode"] = None

        df_standard["AwardsDivisionShortDesc"] = df_raw.get("ak")
        df_standard["CountryOfCTZName"] = df_raw.get("nation")
        df_standard["CountryOfResidenceName"] = df_raw.get("nation")
        df_standard["City"] = None
        df_standard["StateName"] = None
        df_standard["AgeOnRaceDay"] = None

        # Splits e Parciais
        df_standard["Time5K"] = df_raw.get("z5")
        df_standard["Time10K"] = df_raw.get("z10")
        df_standard["Time15K"] = df_raw.get("z15")
        df_standard["Time20K"] = df_raw.get("z20")
        df_standard["TimeHalf"] = df_raw.get("halbmarathon")
        df_standard["Time25K"] = df_raw.get("z25")
        df_standard["Time30K"] = df_raw.get("z30")
        df_standard["Time35K"] = df_raw.get("z35")
        df_standard["Time40K"] = df_raw.get("z40")
        df_standard["ChipFinish"] = df_raw.get("netto")

        # Classificações
        df_standard["RankOverAll"] = pd.to_numeric(
            df_raw.get("platz"), errors="coerce"
        )
        df_standard["RankOverGender"] = pd.to_numeric(
            df_raw.get("sex_platz"), errors="coerce"
        )
        df_standard["RankOverDivision"] = pd.to_numeric(
            df_raw.get("ak_platz"), errors="coerce"
        )

        return df_standard

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR / "berlin",
    ) -> pd.DataFrame:
        """Executa a extração em lote para um intervalo contínuo de anos em Berlim."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            if current_year == 2020:
                logger.info("Ano 2020 pulado (Edição cancelada / COVID-19).")
                continue

            target_path = raw_dir / f"berlin_{current_year}_raw.parquet"

            # Aproveita cache local apenas se a base estiver íntegra (> 5.000 concluintes)
            if target_path.exists():
                df_cached = pd.read_parquet(target_path)
                if len(df_cached) > 5000:
                    logger.info(
                        f"Berlim {current_year} em cache íntegro ({len(df_cached):,} atletas)."
                    )
                    accumulated_frames.append(df_cached)
                    continue
                else:
                    logger.info(
                        f"Berlim {current_year} em cache incompleto ({len(df_cached):,} linhas). Rebaixando..."
                    )
                    target_path.unlink()

            try:
                df_extracted = self.extract_year(
                    current_year, destination_dir=raw_dir
                )
                if not df_extracted.empty:
                    accumulated_frames.append(df_extracted)
            except Exception as exc:
                logger.error(
                    f"Falha na extração de Berlim {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado recuperado para Berlim.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        logger.info(
            f"Extração de Berlim consolidada: {len(df_master):,} atletas no total."
        )
        return df_master