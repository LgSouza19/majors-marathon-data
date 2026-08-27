"""Módulo de extração e padronização de dados da Maratona de Chicago (Mika Timing).

Implementa encerramento explícito de conexões (session.close()), teto determinístico
de paginação e persistência do Raw Lake integral.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import logging
import math
from pathlib import Path
import re
import time
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src import config

logger = logging.getLogger(__name__)

HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",  # Informa ao servidor que não deve manter o socket aberto
}


class ChicagoMarathonExtractor:
    """Cliente HTTP com gerenciamento estrito de ciclo de vida de conexões."""

    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers

    def _create_session(self) -> requests.Session:
        """Cria uma nova sessão isolada com adaptadores de retry."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(
            pool_connections=self.max_workers + 5,
            pool_maxsize=self.max_workers + 5,
            max_retries=retries,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _parse_athlete_card(
        card_soup, base_url: str, year: int, gender_code: str
    ) -> Optional[Dict]:
        name_elem = card_soup.select_one("h4.list-field.type-fullname a")
        if not name_elem or not name_elem.get("href"):
            return None

        full_text = name_elem.get_text(strip=True)
        detail_href = name_elem["href"]

        nation_m = re.search(r"\(([A-Z]{3})\)$", full_text)
        nation = nation_m.group(1) if nation_m else None
        full_name = (
            re.sub(r"\s*\([A-Z]{3}\)$", "", full_text).strip()
            if full_text
            else None
        )

        pl_overall_elem = card_soup.select_one(
            "div.list-field.type-place.place-secondary"
        )
        pl_gender_elem = card_soup.select_one(
            "div.list-field.type-place.place-primary"
        )

        def get_clean_text(selector: str) -> Optional[str]:
            el = card_soup.select_one(selector)
            if not el:
                return None
            for label in el.select(".list-label"):
                label.decompose()
            return el.get_text(strip=True)

        bib = get_clean_text("div.list-field.type-field")
        division = get_clean_text("div.list-field.type-age_class")
        finish_time = get_clean_text("div.list-field.type-time")

        detail_url = (
            base_url + detail_href
            if not detail_href.startswith("http")
            else detail_href
        )

        return {
            "ID_Ref": detail_href,
            "EventYear": year,
            "GenderCode": gender_code,
            "FullBibNumber": bib,
            "FormattedFullName": full_name,
            "CountryOfCTZName": nation,
            "AwardsDivisionShortDesc": division,
            "ChipFinish_List": finish_time,
            "RankOverAll": (
                pl_overall_elem.get_text(strip=True)
                if pl_overall_elem
                else None
            ),
            "RankOverGender": (
                pl_gender_elem.get_text(strip=True) if pl_gender_elem else None
            ),
            "DetailURL": detail_url,
        }

    @staticmethod
    def _fetch_deep_raw_splits(
        athlete: Dict, session: requests.Session
    ) -> Dict:
        raw_record = athlete.copy()
        url = athlete.get("DetailURL")
        if not url:
            return raw_record

        time.sleep(0.015)

        try:
            r = session.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200:
                tables = pd.read_html(io.StringIO(r.text), flavor="lxml")

                for tbl in tables:
                    if len(tbl.columns) == 2 and tbl.columns[0] == 0:
                        for _, row in tbl.iterrows():
                            key = str(row[0]).strip().lower()
                            val = str(row[1]).strip()
                            if "city" in key or "state" in key:
                                raw_record["City_State"] = val
                            elif "start time" in key:
                                raw_record["Start_Time"] = val
                            elif "place age group" in key:
                                raw_record["Place_Age_Group"] = val

                    elif "Split" in tbl.columns and "Time" in tbl.columns:
                        for _, row in tbl.iterrows():
                            split_raw = str(row["Split"]).strip()
                            split_key = (
                                split_raw.replace(" ", "_")
                                .replace(".", "_")
                                .replace("/", "_")
                            )

                            raw_record[f"Time_{split_key}"] = (
                                str(row["Time"]).strip()
                                if pd.notnull(row.get("Time"))
                                else None
                            )

                            if "Diff" in tbl.columns and pd.notnull(
                                row.get("Diff")
                            ):
                                raw_record[f"Diff_{split_key}"] = str(
                                    row["Diff"]
                                ).strip()

                            if "Time Of Day" in tbl.columns and pd.notnull(
                                row.get("Time Of Day")
                            ):
                                raw_record[f"TimeOfDay_{split_key}"] = str(
                                    row["Time Of Day"]
                                ).strip()
        except Exception:
            pass
        return raw_record

    def _fetch_year_listings(
        self, year: int, base_url: str, base_params: dict
    ) -> List[Dict]:
        """Extrai as listagens garantindo que o ano não seja abortado por bloqueio temporário."""
        athletes_list: List[Dict] = []
        seen_hrefs = set()
        genders_to_crawl = [("M", "M"), ("W", "F")]

        for api_sex, gender_label in genders_to_crawl:
            total_athletes = 0

            # Loop de espera contínua até o servidor responder com dados reais
            while True:
                sess = self._create_session()
                params_init = {
                    "pid": "list",
                    "page": "1",
                    "num_results": "100",
                    "search[sex]": api_sex,
                    **base_params,
                }

                try:
                    r_init = sess.get(
                        base_url,
                        headers=HEADERS,
                        params=params_init,
                        timeout=15,
                    )

                    if r_init.status_code == 200:
                        soup_init = BeautifulSoup(r_init.text, "html.parser")
                        info_text = soup_init.find(
                            class_=lambda c: c and "str_num" in str(c)
                        )
                        if info_text:
                            m = re.search(
                                r"(\d+)",
                                info_text.text.replace(".", "").replace(
                                    ",", ""
                                ),
                            )
                            if m:
                                total_athletes = int(m.group(1))
                                sess.close()
                                break

                        # Se encontrou cards válidos na página 1
                        valid_cards = [
                            c
                            for c in soup_init.select("li.list-group-item.row")
                            if "list-group-header" not in c.get("class", [])
                        ]
                        if valid_cards:
                            total_athletes = 50000
                            sess.close()
                            break

                    sess.close()
                except Exception:
                    try:
                        sess.close()
                    except Exception:
                        pass

                logger.warning(
                    f"Aguardando liberação de conexão para Chicago {year} ({gender_label}). Pausa de 20s..."
                )
                time.sleep(20)

            max_pages = (
                math.ceil(total_athletes / 100)
                if total_athletes < 50000
                else 400
            )

            # Paginação protegida
            with tqdm(
                total=max_pages,
                desc=f"Lista Chicago {year} ({gender_label})",
                unit=" pgs",
            ) as pbar:
                page = 1
                consecutive_empty = 0
                list_session = self._create_session()

                while page <= max_pages:
                    params = {
                        "pid": "list",
                        "page": str(page),
                        "num_results": "100",
                        "search[sex]": api_sex,
                        **base_params,
                    }

                    try:
                        resp = list_session.get(
                            base_url, headers=HEADERS, params=params, timeout=15
                        )

                        if resp.status_code == 429:
                            time.sleep(15)
                            continue

                        if resp.status_code != 200:
                            consecutive_empty += 1
                            if consecutive_empty >= 3:
                                break
                            time.sleep(3)
                            continue

                        soup = BeautifulSoup(resp.text, "html.parser")
                        cards = soup.select("li.list-group-item.row")

                        page_new_athletes = 0
                        for card in cards:
                            if "list-group-header" in card.get("class", []):
                                continue
                            parsed = self._parse_athlete_card(
                                card, base_url, year, gender_label
                            )
                            if parsed and parsed["ID_Ref"] not in seen_hrefs:
                                seen_hrefs.add(parsed["ID_Ref"])
                                athletes_list.append(parsed)
                                page_new_athletes += 1

                        if page_new_athletes == 0:
                            consecutive_empty += 1
                            if consecutive_empty >= 2:
                                pbar.update(max_pages - page + 1)
                                break
                        else:
                            consecutive_empty = 0

                        pbar.update(1)
                        page += 1
                        time.sleep(0.04)

                    except Exception:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            break
                        time.sleep(3)

                list_session.close()

        return athletes_list

    def extract_year(
        self, year: int, destination_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """Executa a extração completa de um ano com fechamento estrito de sessões."""
        if year >= 2025:
            base_url = f"https://results.chicagomarathon.com/{year}/"
            base_params = {"event": "MAR"}
        else:
            base_url = "https://chicago-history.r.mikatiming.com/2024/"
            base_params = {"event_main_group": str(year)}

        logger.info(f"Extraindo Chicago Marathon {year}...")

        # 1. Varredura da listagem
        athletes_list = self._fetch_year_listings(year, base_url, base_params)

        if not athletes_list:
            logger.warning(
                f"Ano {year}: Nenhum participante recuperado na listagem."
            )
            return pd.DataFrame()

        logger.info(
            f"Listagem concluída: {len(athletes_list):,} atletas localizados. Extraindo splits..."
        )

        # 2. Extração Concorrente de Splits
        enriched_records: List[Dict] = []
        worker_session = self._create_session()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._fetch_deep_raw_splits, athlete, worker_session
                )
                for athlete in athletes_list
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Splits Chicago {year}",
                unit=" atletas",
            ):
                enriched_records.append(future.result())

        # Fechamento e destruição da sessão de workers para liberar conexões
        worker_session.close()

        df_raw = pd.DataFrame(enriched_records)

        # 3. Persistência do Raw Lake Integral
        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"chicago_{year}_raw.parquet"
            df_raw.astype(str).to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} persistido (raw profundo): {len(df_raw):,} atletas salvos em {output_file.name}"
            )

        # 4. Padronização Canônica
        df_standard = pd.DataFrame()
        df_standard["ID"] = [
            f"CHI_{year}_{i+1:06d}" for i in range(len(df_raw))
        ]
        df_standard["EventYear"] = int(year)
        df_standard["FullBibNumber"] = df_raw.get(
            "FullBibNumber", ""
        ).astype(str)
        df_standard["FormattedFullName"] = df_raw.get("FormattedFullName")
        df_standard["GenderCode"] = df_raw.get("GenderCode")
        df_standard["AwardsDivisionShortDesc"] = df_raw.get(
            "AwardsDivisionShortDesc"
        )
        df_standard["CountryOfCTZName"] = df_raw.get("CountryOfCTZName")
        df_standard["CountryOfResidenceName"] = df_raw.get("CountryOfCTZName")
        df_standard["City"] = df_raw.get("City_State")
        df_standard["StateName"] = None
        df_standard["AgeOnRaceDay"] = None

        df_standard["Time5K"] = df_raw.get("Time_05K", df_raw.get("Time_5K"))
        df_standard["Time10K"] = df_raw.get("Time_10K")
        df_standard["Time15K"] = df_raw.get("Time_15K")
        df_standard["Time20K"] = df_raw.get("Time_20K")
        df_standard["TimeHalf"] = df_raw.get("Time_HALF")
        df_standard["Time25K"] = df_raw.get("Time_25K")
        df_standard["Time30K"] = df_raw.get("Time_30K")
        df_standard["Time35K"] = df_raw.get("Time_35K")
        df_standard["Time40K"] = df_raw.get("Time_40K")
        df_standard["ChipFinish"] = df_raw.get(
            "Time_Finish", df_raw.get("ChipFinish_List")
        )

        df_standard["RankOverAll"] = pd.to_numeric(
            df_raw.get("RankOverAll"), errors="coerce"
        )
        df_standard["RankOverGender"] = pd.to_numeric(
            df_raw.get("RankOverGender"), errors="coerce"
        )
        df_standard["RankOverDivision"] = None

        # Pausa de 15s para resfriar a conexão com o servidor
        logger.info(
            f"Aguardando 15s para resfriamento de conexão antes do próximo ano..."
        )
        time.sleep(15)

        return df_standard

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR / "chicago",
    ) -> pd.DataFrame:
        """Executa a extração em lote para um intervalo contínuo de anos em Chicago."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            if current_year == 2020:
                logger.info("Ano 2020 pulado (Edição cancelada / COVID-19).")
                continue

            target_path = raw_dir / f"chicago_{current_year}_raw.parquet"

            if target_path.exists():
                try:
                    df_cached = pd.read_parquet(target_path)
                    if len(df_cached) > 2000:
                        logger.info(
                            f"Chicago {current_year} em cache ({len(df_cached):,} atletas)."
                        )
                        df_std_cached = pd.DataFrame()
                        df_std_cached["ID"] = [
                            f"CHI_{current_year}_{i+1:06d}"
                            for i in range(len(df_cached))
                        ]
                        df_std_cached["EventYear"] = int(current_year)
                        df_std_cached["FullBibNumber"] = df_cached.get(
                            "FullBibNumber", ""
                        ).astype(str)
                        df_std_cached["FormattedFullName"] = df_cached.get(
                            "FormattedFullName"
                        )
                        df_std_cached["GenderCode"] = df_cached.get(
                            "GenderCode"
                        )
                        df_std_cached["AwardsDivisionShortDesc"] = (
                            df_cached.get("AwardsDivisionShortDesc")
                        )
                        df_std_cached["CountryOfCTZName"] = df_cached.get(
                            "CountryOfCTZName"
                        )
                        df_std_cached["CountryOfResidenceName"] = df_cached.get(
                            "CountryOfCTZName"
                        )
                        df_std_cached["City"] = df_cached.get("City_State")
                        df_std_cached["StateName"] = None
                        df_std_cached["AgeOnRaceDay"] = None

                        df_std_cached["Time5K"] = df_cached.get(
                            "Time_05K", df_cached.get("Time_5K")
                        )
                        df_std_cached["Time10K"] = df_cached.get("Time_10K")
                        df_std_cached["Time15K"] = df_cached.get("Time_15K")
                        df_std_cached["Time20K"] = df_cached.get("Time_20K")
                        df_std_cached["TimeHalf"] = df_cached.get("Time_HALF")
                        df_std_cached["Time25K"] = df_cached.get("Time_25K")
                        df_std_cached["Time30K"] = df_cached.get("Time_30K")
                        df_std_cached["Time35K"] = df_cached.get("Time_35K")
                        df_std_cached["Time40K"] = df_cached.get("Time_40K")
                        df_std_cached["ChipFinish"] = df_cached.get(
                            "Time_Finish", df_cached.get("ChipFinish_List")
                        )

                        df_std_cached["RankOverAll"] = pd.to_numeric(
                            df_cached.get("RankOverAll"), errors="coerce"
                        )
                        df_std_cached["RankOverGender"] = pd.to_numeric(
                            df_cached.get("RankOverGender"), errors="coerce"
                        )
                        df_std_cached["RankOverDivision"] = None

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
                    f"Falha na extração de Chicago {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado recuperado para Chicago.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        logger.info(
            f"Extração de Chicago consolidada: {len(df_master):,} atletas no total."
        )
        return df_master