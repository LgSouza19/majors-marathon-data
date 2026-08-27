"""Módulo de extração e padronização de dados da Maratona de Nova York (TCS New York City Marathon - NYRR).

Implementa extração completa de todas as 55 edições (1970 a 2025) via paginação fatiada
por classificação (Ranking Slicing), superando a trava de 500 registros da NYRR.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import math
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

NYRR_FINISHERS_URL: str = "https://rmsprodapi.nyrr.org/api/v2/runners/finishers-filter"
NYRR_DETAILS_URL: str = "https://rmsprodapi.nyrr.org/api/v2/runners/resultDetails"

HEADERS_NY: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=utf-8",
    "Origin": "https://results.nyrr.org",
    "Referer": "https://results.nyrr.org/",
    "Connection": "close",
}

EVENT_MAP_NY: Dict[int, str] = {
    # 2014 a 2025 (Era Moderna)
    2025: "M2025",
    2024: "M2024",
    2023: "M2023",
    2022: "M2022",
    2021: "M2021",
    2019: "M2019",
    2018: "M2018",
    2017: "M2017",
    2016: "M2016",
    2015: "M2015",
    2014: "M2014",
    # 2000 a 2013 (Era de Transição / IDs Internos)
    2013: "40",
    2011: "108",
    2010: "b01107",
    2009: "a91101",
    2008: "a81102",
    2007: "a71104",
    2006: "a61105",
    2005: "a51106",
    2004: "a41107",
    2003: "NYC2003",
    2002: "NYC2002",
    2001: "b11106",
    2000: "NYC2000",
    # 1970 a 1999 (Era Clássica do Século XX - AAMMDD)
    1999: "991107",
    1998: "981101",
    1997: "971102",
    1996: "961103",
    1995: "951112",
    1994: "941106",
    1993: "931114",
    1992: "921101",
    1991: "911103",
    1990: "901104",
    1989: "891105",
    1988: "881106",
    1987: "871101",
    1986: "861102",
    1985: "851027",
    1984: "841028",
    1983: "831023",
    1982: "821024",
    1981: "811025",
    1980: "801026",
    1979: "791021",
    1978: "781022",
    1977: "771023",
    1976: "761024",
    1975: "750928",
    1974: "740929",
    1973: "730930",
    1972: "721001",
    1971: "710919",
    1970: "700913",
}


class NewYorkMarathonExtractor:
    """Cliente HTTP com suporte a ranking slicing e multithreading para Nova York."""

    def __init__(self, max_workers: int = 20, page_size: int = 100):
        self.max_workers = max_workers
        self.page_size = page_size

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"],
        )
        adapter = HTTPAdapter(
            pool_connections=self.max_workers + 10,
            pool_maxsize=self.max_workers + 10,
            max_retries=retries,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _fetch_athlete_splits(
        athlete: Dict, event_code: str, session: requests.Session
    ) -> Dict:
        raw_record = athlete.copy()
        runner_id = athlete.get("runnerId")
        if not runner_id:
            return raw_record

        payload = {
            "eventCode": event_code,
            "runnerId": runner_id,
        }

        try:
            r = session.post(
                NYRR_DETAILS_URL, headers=HEADERS_NY, json=payload, timeout=12
            )
            if r.status_code == 200:
                data = r.json()
                details = data.get("details")
                if details:
                    raw_record["teamName"] = details.get("teamName")
                    raw_record["gunTime"] = details.get("gunTime")
                    raw_record["netTime"] = details.get("netTime")
                    raw_record["percentAgeGrade"] = details.get("percentAgeGrade")
                    raw_record["ageGroupFromTo"] = details.get("ageGroupFromTo")

                    for sp_item in details.get("splitResults", []):
                        sp_code = sp_item.get("splitCode")
                        sp_time = sp_item.get("time")
                        sp_pace = sp_item.get("pace")
                        sp_speed = sp_item.get("speed")

                        if sp_code:
                            raw_record[f"Time_{sp_code}"] = sp_time
                            raw_record[f"Pace_{sp_code}"] = sp_pace
                            raw_record[f"Speed_{sp_code}"] = sp_speed
        except Exception:
            pass
        return raw_record

    def _fetch_year_finishers(
        self, event_code: str, year: int
    ) -> List[Dict]:
        """Extrai todos os concluintes de Nova York fatiando por blocos de 500 colocações."""
        sess = self._create_session()
        payload_init = {
            "eventCode": event_code,
            "pageIndex": 1,
            "pageSize": 1,
            "sortColumn": "overallTime",
            "sortDescending": False,
        }

        total_items = 0
        try:
            r_init = sess.post(
                NYRR_FINISHERS_URL, headers=HEADERS_NY, json=payload_init, timeout=15
            )
            if r_init.status_code == 200:
                total_items = r_init.json().get("totalItems", 0)
        except Exception as e:
            logger.error(f"Erro ao inicializar NY {year}: {e}")
            sess.close()
            return []

        if total_items == 0:
            sess.close()
            return []

        logger.info(
            f"Nova York {year}: {total_items:,} concluintes mapeados. "
            f"Iniciando varredura por fatiamento de colocação (blocos de 500)..."
        )

        athletes: List[Dict] = []
        seen_ids = set()

        # Fatiamento em blocos de 500 colocações
        slice_size = 500
        total_slices = math.ceil(total_items / slice_size)

        with tqdm(
            total=total_items,
            desc=f"Listagem NY {year}",
            unit=" atletas",
        ) as pbar:
            for s_idx in range(total_slices):
                p_from = s_idx * slice_size + 1
                p_to = min((s_idx + 1) * slice_size, total_items)

                # Dentro de cada bloco de 500, pagina de 100 em 100 (páginas 1 a 5)
                max_pages_in_slice = math.ceil((p_to - p_from + 1) / self.page_size)

                for page in range(1, max_pages_in_slice + 1):
                    payload_slice = {
                        "eventCode": event_code,
                        "overallPlaceFrom": p_from,
                        "overallPlaceTo": p_to,
                        "sortColumn": "overallPlace",
                        "sortDescending": False,
                        "pageIndex": page,
                        "pageSize": self.page_size,
                    }

                    try:
                        resp = sess.post(
                            NYRR_FINISHERS_URL,
                            headers=HEADERS_NY,
                            json=payload_slice,
                            timeout=15,
                        )
                        if resp.status_code == 200:
                            batch = resp.json().get("items", [])
                            if not batch:
                                break

                            page_new = 0
                            for ath in batch:
                                r_id = ath.get("runnerId")
                                if r_id and r_id not in seen_ids:
                                    seen_ids.add(r_id)
                                    athletes.append(ath)
                                    page_new += 1

                            pbar.update(page_new)
                        time.sleep(0.02)
                    except Exception as exc:
                        logger.warning(f"Falha no bloco {p_from}-{p_to} (pág {page}) de NY {year}: {exc}")

        sess.close()
        return athletes

    def extract_year(
        self, year: int, destination_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """Executa a extração completa de uma edição de Nova York."""
        if year in [2012, 2020]:
            logger.warning(
                f"Ano {year} pulado (Edição cancelada oficialmente - Sandy/COVID)."
            )
            return pd.DataFrame()

        if year not in EVENT_MAP_NY:
            logger.warning(f"Ano {year} não cadastrado no mapa de eventos da NYRR.")
            return pd.DataFrame()

        event_code = EVENT_MAP_NY[year]
        logger.info(f"Extraindo TCS New York City Marathon {year} ({event_code})...")

        # 1. Extração da listagem completa sem limite de 500
        athletes_list = self._fetch_year_finishers(event_code, year)

        if not athletes_list:
            logger.warning(f"Ano {year}: Nenhum participante recuperado.")
            return pd.DataFrame()

        # 2. Extração Concorrente de Splits (apenas para edições com parciais eletrônicas)
        enriched_records: List[Dict] = []
        worker_session = self._create_session()

        if year >= 1999:
            logger.info(
                f"Listagem concluída: {len(athletes_list):,} atletas localizados. "
                f"Extraindo splits de 5 km..."
            )
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._fetch_athlete_splits,
                        athlete,
                        event_code,
                        worker_session,
                    )
                    for athlete in athletes_list
                ]

                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"Splits NY {year}",
                    unit=" atletas",
                ):
                    enriched_records.append(future.result())
        else:
            enriched_records = athletes_list

        worker_session.close()
        df_raw = pd.DataFrame(enriched_records)

        # 3. Persistência do Raw Lake Integral
        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"ny_{year}_raw.parquet"
            df_raw.astype(str).to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} persistido (raw profundo): {len(df_raw):,} atletas salvos em {output_file.name}"
            )

        # 4. Padronização Canônica Blindada
        df_standard = pd.DataFrame()
        df_standard["ID"] = [f"NYC_{year}_{i+1:06d}" for i in range(len(df_raw))]
        df_standard["EventYear"] = int(year)
        df_standard["FullBibNumber"] = df_raw.get("bib", "").fillna("").astype(str)

        first_names = df_raw.get("firstName", "").fillna("").astype(str)
        last_names = df_raw.get("lastName", "").fillna("").astype(str)
        df_standard["FormattedFullName"] = (first_names + " " + last_names).str.strip()

        df_standard["GenderCode"] = df_raw.get("gender")
        df_standard["AwardsDivisionShortDesc"] = df_raw.get("ageGroupFromTo")
        df_standard["CountryOfCTZName"] = df_raw.get("countryCode", df_raw.get("iaaf"))
        df_standard["CountryOfResidenceName"] = df_raw.get("countryCode")
        df_standard["City"] = df_raw.get("city")
        df_standard["StateName"] = df_raw.get("stateProvince")
        df_standard["AgeOnRaceDay"] = pd.to_numeric(df_raw.get("age"), errors="coerce")

        # Splits Canônicos de 5k
        df_standard["Time5K"] = df_raw.get("Time_5K")
        df_standard["Time10K"] = df_raw.get("Time_10K")
        df_standard["Time15K"] = df_raw.get("Time_15K")
        df_standard["Time20K"] = df_raw.get("Time_20K")
        df_standard["TimeHalf"] = df_raw.get("Time_HALF")
        df_standard["Time25K"] = df_raw.get("Time_25K")
        df_standard["Time30K"] = df_raw.get("Time_30K")
        df_standard["Time35K"] = df_raw.get("Time_35K")
        df_standard["Time40K"] = df_raw.get("Time_40K")
        df_standard["ChipFinish"] = df_raw.get(
            "Time_MAR", df_raw.get("netTime", df_raw.get("overallTime"))
        )

        df_standard["RankOverAll"] = pd.to_numeric(df_raw.get("overallPlace"), errors="coerce")
        df_standard["RankOverGender"] = pd.to_numeric(df_raw.get("genderPlace"), errors="coerce")
        df_standard["RankOverDivision"] = pd.to_numeric(df_raw.get("placeAgeGroup"), errors="coerce")

        time.sleep(2)
        return df_standard

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR / "ny",
    ) -> pd.DataFrame:
        """Executa a extração em lote para um intervalo contínuo de anos em Nova York."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            if current_year in [2012, 2020]:
                logger.info(
                    f"Ano {current_year} pulado (Edição cancelada oficialmente - Sandy/COVID)."
                )
                continue

            target_path = raw_dir / f"ny_{current_year}_raw.parquet"

            if target_path.exists():
                try:
                    df_cached = pd.read_parquet(target_path)
                    # 1970 teve 55 concluintes, anos modernos têm > 20.000
                    min_finishers = 50 if current_year <= 1975 else 5000
                    if len(df_cached) >= min_finishers:
                        logger.info(
                            f"Nova York {current_year} em cache íntegro ({len(df_cached):,} atletas)."
                        )
                        df_std_cached = pd.DataFrame()
                        df_std_cached["ID"] = [
                            f"NYC_{current_year}_{i+1:06d}"
                            for i in range(len(df_cached))
                        ]
                        df_std_cached["EventYear"] = int(current_year)
                        df_std_cached["FullBibNumber"] = df_cached.get("bib", "").fillna("").astype(str)

                        f_names = df_cached.get("firstName", "").fillna("").astype(str)
                        l_names = df_cached.get("lastName", "").fillna("").astype(str)
                        df_std_cached["FormattedFullName"] = (f_names + " " + l_names).str.strip()

                        df_std_cached["GenderCode"] = df_cached.get("gender")
                        df_std_cached["AwardsDivisionShortDesc"] = df_cached.get("ageGroupFromTo")
                        df_std_cached["CountryOfCTZName"] = df_cached.get("countryCode", df_cached.get("iaaf"))
                        df_std_cached["CountryOfResidenceName"] = df_cached.get("countryCode")
                        df_std_cached["City"] = df_cached.get("city")
                        df_std_cached["StateName"] = df_cached.get("stateProvince")
                        df_std_cached["AgeOnRaceDay"] = pd.to_numeric(df_cached.get("age"), errors="coerce")

                        df_std_cached["Time5K"] = df_cached.get("Time_5K")
                        df_std_cached["Time10K"] = df_cached.get("Time_10K")
                        df_std_cached["Time15K"] = df_cached.get("Time_15K")
                        df_std_cached["Time20K"] = df_cached.get("Time_20K")
                        df_std_cached["TimeHalf"] = df_cached.get("Time_HALF")
                        df_std_cached["Time25K"] = df_cached.get("Time_25K")
                        df_std_cached["Time30K"] = df_cached.get("Time_30K")
                        df_std_cached["Time35K"] = df_cached.get("Time_35K")
                        df_std_cached["Time40K"] = df_cached.get("Time_40K")
                        df_std_cached["ChipFinish"] = df_cached.get(
                            "Time_MAR", df_cached.get("netTime", df_cached.get("overallTime"))
                        )

                        df_std_cached["RankOverAll"] = pd.to_numeric(df_cached.get("overallPlace"), errors="coerce")
                        df_std_cached["RankOverGender"] = pd.to_numeric(df_cached.get("genderPlace"), errors="coerce")
                        df_std_cached["RankOverDivision"] = pd.to_numeric(df_cached.get("placeAgeGroup"), errors="coerce")

                        accumulated_frames.append(df_std_cached)
                        continue
                    else:
                        target_path.unlink()
                except Exception:
                    target_path.unlink()

            try:
                df_extracted = self.extract_year(
                    current_year, destination_dir=raw_dir
                )
                if not df_extracted.empty:
                    accumulated_frames.append(df_extracted)
            except Exception as exc:
                logger.error(
                    f"Falha na extração de Nova York {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado recuperado para Nova York.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        logger.info(
            f"Extração de Nova York consolidada: {len(df_master):,} atletas no total."
        )
        return df_master