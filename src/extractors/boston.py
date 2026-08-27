"""Módulo de extração de dados da API da Boston Athletic Association (B.A.A.)."""

import logging
from pathlib import Path
import time
from typing import Dict, List, Optional
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src import config

logger = logging.getLogger(__name__)


class BostonMarathonExtractor:
    """Cliente HTTP resiliente para ingestão de resultados da Maratona de Boston."""

    def __init__(self, page_size: int = 75):
        # 75 é o tamanho máximo de lote entregue pelo servidor da API
        self.page_size = page_size
        self.session = self._build_retry_session()

    @staticmethod
    def _build_retry_session() -> requests.Session:
        """Configura uma sessão HTTP com política de retry para erros transientes."""
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

    def extract_year(
        self, year: int, destination_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """Extrai todos os maratonistas de uma edição paginando em lotes de 75."""
        offset = 0
        records: List[Dict] = []
        columns_selector = ",".join(config.RAW_COLUMNS)

        with tqdm(desc=f"Boston {year}", unit=" atletas") as pbar:
            while True:
                params = {
                    "select": columns_selector,
                    "ShortDescription": f"ilike.*{year} Boston Marathon*",
                    "ReportingSegment": "in.(Runners,runner)",
                    "order": "RankOverAll.asc.nullslast",
                    "offset": str(offset),
                    "limit": str(self.page_size),
                }

                resp = self.session.get(
                    config.BASE_URL,
                    headers=config.DEFAULT_HEADERS,
                    params=params,
                    timeout=20,
                )
                resp.raise_for_status()
                batch_data = resp.json()

                # Para apenas quando a base não tiver mais nenhum registro
                if not batch_data:
                    break

                records.extend(batch_data)
                pbar.update(len(batch_data))

                # Incrementa o offset e continua paginando
                offset += len(batch_data)
                time.sleep(0.02)

        if not records:
            logger.warning(f"Ano {year}: Nenhum participante localizado.")
            return pd.DataFrame()

        df_year = pd.DataFrame(records)

        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"boston_{year}_raw.parquet"
            df_year.to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} concluído: {len(df_year):,} atletas salvos em {output_file.name}"
            )

        return df_year

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR,
    ) -> pd.DataFrame:
        """Executa a extração em lote para um intervalo contínuo de anos."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            target_path = raw_dir / f"boston_{current_year}_raw.parquet"

            # Se o arquivo já existe e está completo, aproveita do cache
            if target_path.exists():
                df_cached = pd.read_parquet(target_path)
                if len(df_cached) > 500:
                    logger.info(
                        f"Ano {current_year} em cache ({len(df_cached):,} atletas)."
                    )
                    accumulated_frames.append(df_cached)
                    continue
                else:
                    target_path.unlink()

            try:
                df_extracted = self.extract_year(
                    current_year, destination_dir=raw_dir
                )
                if not df_extracted.empty:
                    accumulated_frames.append(df_extracted)
            except Exception as exc:
                logger.error(
                    f"Falha na extração da edição de {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado foi recuperado no intervalo definido.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        logger.info(
            f"Extração consolidada: {len(df_master):,} registros no total."
        )
        return df_master