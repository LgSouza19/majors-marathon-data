"""Módulo de extração e padronização de dados da Maratona de Londres (TCS London Marathon).

Suporta de forma polimórfica a Era Clássica (2001-2013, tabelas HTML) e a Era Moderna
(2014-2026, cards responsivos), com captura profunda para o Raw Lake e recuperação adaptativa.
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
    "Connection": "close",
}


def _safe_get_series(
    df: pd.DataFrame, col: str, fallback_col: Optional[str] = None
) -> pd.Series:
    """Recupera uma coluna com fallback seguro sem gerar erro de NoneType."""
    if col in df.columns and df[col].notnull().any():
        return df[col]
    if fallback_col and fallback_col in df.columns:
        return df[fallback_col]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


class LondonMarathonExtractor:
    """Cliente HTTP resiliente para todas as eras da Maratona de Londres."""

    def __init__(self, max_workers: int = 15):
        self.max_workers = max_workers

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.0,
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
    def _get_base_url(year: int) -> str:
        """Roteia para o portal histórico (2001-2013) ou moderno (2014+)."""
        if year >= 2014:
            return f"https://results.tcslondonmarathon.com/{year}/"
        return f"https://london-history.r.mikatiming.com/{year}/"

    def _resolve_event_code(self, base_url: str) -> Optional[str]:
        """Descobre o identificador da Maratona no dropdown da página."""
        sess = self._create_session()
        try:
            r = sess.get(
                base_url, headers=HEADERS, params={"pid": "list"}, timeout=10
            )
            sess.close()
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                select_event = soup.find("select", {"name": "event"})
                if select_event:
                    for opt in select_event.find_all("option"):
                        val = opt.get("value", "")
                        txt = opt.text.strip().lower()
                        if any(
                            k in txt for k in ["marathon", "mass", "masses"]
                        ) and not any(
                            k in txt
                            for k in ["wheelchair", "handcycle", "mini", "elite"]
                        ):
                            return val
                    if select_event.find_all("option"):
                        return select_event.find_all("option")[0].get("value")
        except Exception:
            try:
                sess.close()
            except Exception:
                pass
        return None

    @staticmethod
    def _parse_row_unified(
        element, base_url: str, year: int, gender_code: str
    ) -> Optional[Dict]:
        """Extrai dados tanto de cards modernos (<li>) quanto de tabelas clássicas (<tr>)."""
        name_elem = element.find(
            "a",
            href=lambda h: h and ("content=detail" in h or "idp=" in h),
        )
        if not name_elem:
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

        detail_url = (
            base_url + detail_href
            if not detail_href.startswith("http")
            else detail_href
        )

        # 1. Card Moderno (2014+)
        if element.name == "li" or "list-group-item" in element.get(
            "class", []
        ):
            pl_overall = element.select_one(
                "div.list-field.type-place.place-secondary"
            )
            pl_gender = element.select_one(
                "div.list-field.type-place.place-primary"
            )

            def get_clean_text(selector: str) -> Optional[str]:
                el = element.select_one(selector)
                if not el:
                    return None
                for label in el.select(".list-label"):
                    label.decompose()
                return el.get_text(strip=True)

            bib = get_clean_text("div.list-field.type-field")
            division = get_clean_text("div.list-field.type-age_class")
            finish_time = get_clean_text("div.list-field.type-time")

            return {
                "ID_Ref": detail_href,
                "EventYear": year,
                "GenderCode": gender_code,
                "FullBibNumber_List": bib,
                "FormattedFullName": full_name,
                "CountryOfCTZName": nation,
                "AwardsDivisionShortDesc": division,
                "ChipFinish_List": finish_time,
                "RankOverAll": (
                    pl_overall.get_text(strip=True) if pl_overall else None
                ),
                "RankOverGender": (
                    pl_gender.get_text(strip=True) if pl_gender else None
                ),
                "DetailURL": detail_url,
            }

        # 2. Tabela Clássica (2001-2013)
        elif element.name == "tr":
            tds = element.find_all("td")
            if not tds:
                return None
            td_texts = [td.get_text(strip=True) for td in tds]
            place_raw = td_texts[0] if len(td_texts) > 0 else None
            finish_raw = td_texts[-1] if len(td_texts) > 1 else None

            return {
                "ID_Ref": detail_href,
                "EventYear": year,
                "GenderCode": gender_code,
                "FullBibNumber_List": None,
                "FormattedFullName": full_name,
                "CountryOfCTZName": nation,
                "AwardsDivisionShortDesc": None,
                "ChipFinish_List": finish_raw,
                "RankOverAll": place_raw,
                "RankOverGender": None,
                "DetailURL": detail_url,
            }

        return None

    @staticmethod
    def _fetch_deep_raw_splits(
        athlete: Dict, session: requests.Session
    ) -> Dict:
        """Extrai todas as tabelas e parciais da página de detalhes do atleta."""
        raw_record = athlete.copy()
        url = athlete.get("DetailURL")
        if not url:
            return raw_record

        time.sleep(0.01)

        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                tables = pd.read_html(io.StringIO(r.text), flavor="lxml")

                for tbl in tables:
                    if len(tbl.columns) == 2 and tbl.columns[0] == 0:
                        for _, row in tbl.iterrows():
                            k = (
                                str(row[0])
                                .strip()
                                .lower()
                                .replace(" ", "_")
                                .replace(":", "")
                            )
                            v = str(row[1]).strip()
                            raw_record[f"Meta_{k}"] = v

                    elif "Split" in tbl.columns and "Time" in tbl.columns:
                        for _, row in tbl.iterrows():
                            sp = (
                                str(row["Split"])
                                .strip()
                                .replace(" ", "_")
                                .replace(".", "_")
                            )

                            val_time = str(row["Time"]).strip()
                            raw_record[f"Time_{sp}"] = (
                                val_time
                                if val_time not in ["-", "", "None"]
                                else None
                            )

                            if "Diff" in tbl.columns and pd.notnull(
                                row.get("Diff")
                            ):
                                raw_record[f"Diff_{sp}"] = str(
                                    row["Diff"]
                                ).strip()

                            if "Time Of Day" in tbl.columns and pd.notnull(
                                row.get("Time Of Day")
                            ):
                                raw_record[f"TimeOfDay_{sp}"] = str(
                                    row["Time Of Day"]
                                ).strip()

                            if "min/km" in tbl.columns and pd.notnull(
                                row.get("min/km")
                            ):
                                raw_record[f"PaceMinKm_{sp}"] = str(
                                    row["min/km"]
                                ).strip()

                            if "km/h" in tbl.columns and pd.notnull(
                                row.get("km/h")):
                                raw_record[f"SpeedKmH_{sp}"] = str(
                                    row["km/h"]
                                ).strip()

                            if "Place" in tbl.columns and pd.notnull(
                                row.get("Place")
                            ):
                                raw_record[f"Place_{sp}"] = str(
                                    row["Place"]
                                ).strip()
        except Exception:
            pass
        return raw_record

    def _fetch_year_listings(
        self, year: int, base_url: str, event_code: Optional[str]
    ) -> List[Dict]:
        """Varre as listagens com suporte polimórfico e recuperação de cooldown."""
        athletes_list: List[Dict] = []
        seen_hrefs = set()
        genders_to_crawl = [("M", "M"), ("W", "F")]

        for api_sex, gender_label in genders_to_crawl:
            total_athletes = 0

            # Loop de espera contínua até o servidor responder
            while True:
                sess = self._create_session()
                params_init = {
                    "pid": "list",
                    "page": "1",
                    "num_results": "100",
                    "search[sex]": api_sex,
                    "search[age_class]": "%",
                }
                if event_code:
                    params_init["event"] = event_code

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

                        # Fallback por detecção de elementos na pág 1
                        if (
                            soup_init.select("li.list-group-item.row")
                            or soup_init.find_all("tr")
                        ):
                            total_athletes = 45000
                            sess.close()
                            break

                    sess.close()
                except Exception:
                    try:
                        sess.close()
                    except Exception:
                        pass

                logger.warning(
                    f"Aguardando liberação de conexão para Londres {year} ({gender_label}). Pausa de 15s..."
                )
                time.sleep(15)

            max_pages = math.ceil(total_athletes / 100)

            # Paginação determinística
            with tqdm(
                total=max_pages,
                desc=f"Lista Londres {year} ({gender_label})",
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
                        "search[age_class]": "%",
                    }
                    if event_code:
                        params["event"] = event_code

                    try:
                        resp = list_session.get(
                            base_url, headers=HEADERS, params=params, timeout=15
                        )
                        if resp.status_code == 429:
                            time.sleep(10)
                            continue

                        if resp.status_code != 200:
                            consecutive_empty += 1
                            if consecutive_empty >= 3:
                                break
                            time.sleep(2)
                            continue

                        soup = BeautifulSoup(resp.text, "html.parser")
                        elements = soup.select("li.list-group-item.row")
                        if not elements:
                            table = soup.find(
                                "table",
                                class_=lambda c: c and "list" in str(c).lower(),
                            ) or soup.find("table")
                            elements = table.find_all("tr") if table else []

                        page_new = 0
                        for el in elements:
                            if "list-group-header" in el.get("class", []):
                                continue
                            parsed = self._parse_row_unified(
                                el, base_url, year, gender_label
                            )
                            if parsed and parsed["ID_Ref"] not in seen_hrefs:
                                seen_hrefs.add(parsed["ID_Ref"])
                                athletes_list.append(parsed)
                                page_new += 1

                        if page_new == 0:
                            consecutive_empty += 1
                            if consecutive_empty >= 2:
                                pbar.update(max_pages - page + 1)
                                break
                        else:
                            consecutive_empty = 0

                        pbar.update(1)
                        page += 1
                        time.sleep(0.03)

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
        """Executa a extração completa de uma edição de Londres."""
        if year == 2008:
            logger.warning(
                "Ano 2008 pulado (Base desvinculada no servidor da Mika Timing)."
            )
            return pd.DataFrame()

        base_url = self._get_base_url(year)
        event_code = self._resolve_event_code(base_url)

        logger.info(
            f"Extraindo London Marathon {year} (EventCode: {event_code})..."
        )

        # 1. Varredura polimórfica
        athletes_list = self._fetch_year_listings(year, base_url, event_code)

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
                desc=f"Splits Londres {year}",
                unit=" atletas",
            ):
                enriched_records.append(future.result())

        worker_session.close()
        df_raw = pd.DataFrame(enriched_records)

        # 3. Persistência do Raw Lake Integral
        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"london_{year}_raw.parquet"
            df_raw.astype(str).to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} persistido (raw profundo): {len(df_raw):,} atletas salvos em {output_file.name}"
            )

        # 4. Padronização Canônica Blindada
        df_standard = pd.DataFrame()
        df_standard["ID"] = [
            f"LON_{year}_{i+1:06d}" for i in range(len(df_raw))
        ]
        df_standard["EventYear"] = int(year)

        # Acesso seguro às colunas
        full_bib_series = _safe_get_series(
            df_raw, "Meta_runner_number", "FullBibNumber_List"
        )
        df_standard["FullBibNumber"] = full_bib_series.fillna("").astype(str)

        df_standard["FormattedFullName"] = _safe_get_series(
            df_raw, "FormattedFullName"
        )
        df_standard["GenderCode"] = _safe_get_series(df_raw, "GenderCode")
        df_standard["AwardsDivisionShortDesc"] = _safe_get_series(
            df_raw, "Meta_category", "AwardsDivisionShortDesc"
        )
        df_standard["CountryOfCTZName"] = _safe_get_series(
            df_raw, "CountryOfCTZName"
        )
        df_standard["CountryOfResidenceName"] = _safe_get_series(
            df_raw, "CountryOfCTZName"
        )
        df_standard["City"] = _safe_get_series(df_raw, "Meta_club")
        df_standard["StateName"] = None
        df_standard["AgeOnRaceDay"] = None

        df_standard["Time5K"] = _safe_get_series(
            df_raw, "Time_5K", "Time_05K"
        )
        df_standard["Time10K"] = _safe_get_series(df_raw, "Time_10K")
        df_standard["Time15K"] = _safe_get_series(df_raw, "Time_15K")
        df_standard["Time20K"] = _safe_get_series(df_raw, "Time_20K")
        df_standard["TimeHalf"] = _safe_get_series(
            df_raw, "Time_Half", "Time_HALF"
        )
        df_standard["Time25K"] = _safe_get_series(df_raw, "Time_25K")
        df_standard["Time30K"] = _safe_get_series(df_raw, "Time_30K")
        df_standard["Time35K"] = _safe_get_series(df_raw, "Time_35K")
        df_standard["Time40K"] = _safe_get_series(df_raw, "Time_40K")
        df_standard["ChipFinish"] = _safe_get_series(
            df_raw, "Meta_finish_time", "Time_Finish"
        )

        df_standard["RankOverAll"] = pd.to_numeric(
            _safe_get_series(df_raw, "RankOverAll"), errors="coerce"
        )
        df_standard["RankOverGender"] = pd.to_numeric(
            _safe_get_series(df_raw, "RankOverGender"), errors="coerce"
        )
        df_standard["RankOverDivision"] = None

        time.sleep(5)
        return df_standard

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR / "london",
    ) -> pd.DataFrame:
        """Executa a extração em lote para um intervalo contínuo de anos em Londres."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            if current_year == 2008:
                logger.info(
                    "Ano 2008 pulado (Base desvinculada no servidor da Mika Timing)."
                )
                continue

            target_path = raw_dir / f"london_{current_year}_raw.parquet"

            if target_path.exists():
                try:
                    df_cached = pd.read_parquet(target_path)
                    min_finishers = 20 if current_year == 2020 else 5000
                    if len(df_cached) >= min_finishers:
                        logger.info(
                            f"Londres {current_year} em cache ({len(df_cached):,} atletas)."
                        )
                        df_std_cached = pd.DataFrame()
                        df_std_cached["ID"] = [
                            f"LON_{current_year}_{i+1:06d}"
                            for i in range(len(df_cached))
                        ]
                        df_std_cached["EventYear"] = int(current_year)

                        bib_cached = _safe_get_series(
                            df_cached,
                            "Meta_runner_number",
                            "FullBibNumber_List",
                        )
                        df_std_cached["FullBibNumber"] = (
                            bib_cached.fillna("").astype(str)
                        )
                        df_std_cached["FormattedFullName"] = _safe_get_series(
                            df_cached, "FormattedFullName"
                        )
                        df_std_cached["GenderCode"] = _safe_get_series(
                            df_cached, "GenderCode"
                        )
                        df_std_cached["AwardsDivisionShortDesc"] = (
                            _safe_get_series(
                                df_cached,
                                "Meta_category",
                                "AwardsDivisionShortDesc",
                            )
                        )
                        df_std_cached["CountryOfCTZName"] = _safe_get_series(
                            df_cached, "CountryOfCTZName"
                        )
                        df_std_cached["CountryOfResidenceName"] = (
                            _safe_get_series(df_cached, "CountryOfCTZName")
                        )
                        df_std_cached["City"] = _safe_get_series(
                            df_cached, "Meta_club"
                        )
                        df_std_cached["StateName"] = None
                        df_std_cached["AgeOnRaceDay"] = None

                        df_std_cached["Time5K"] = _safe_get_series(
                            df_cached, "Time_5K", "Time_05K"
                        )
                        df_std_cached["Time10K"] = _safe_get_series(
                            df_cached, "Time_10K"
                        )
                        df_std_cached["Time15K"] = _safe_get_series(
                            df_cached, "Time_15K"
                        )
                        df_std_cached["Time20K"] = _safe_get_series(
                            df_cached, "Time_20K"
                        )
                        df_std_cached["TimeHalf"] = _safe_get_series(
                            df_cached, "Time_Half", "Time_HALF"
                        )
                        df_std_cached["Time25K"] = _safe_get_series(
                            df_cached, "Time_25K"
                        )
                        df_std_cached["Time30K"] = _safe_get_series(
                            df_cached, "Time_30K"
                        )
                        df_std_cached["Time35K"] = _safe_get_series(
                            df_cached, "Time_35K"
                        )
                        df_std_cached["Time40K"] = _safe_get_series(
                            df_cached, "Time_40K"
                        )
                        df_std_cached["ChipFinish"] = _safe_get_series(
                            df_cached, "Meta_finish_time", "Time_Finish"
                        )

                        df_std_cached["RankOverAll"] = pd.to_numeric(
                            _safe_get_series(df_cached, "RankOverAll"),
                            errors="coerce",
                        )
                        df_std_cached["RankOverGender"] = pd.to_numeric(
                            _safe_get_series(df_cached, "RankOverGender"),
                            errors="coerce",
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
                    f"Falha na extração de Londres {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado recuperado para Londres.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        logger.info(
            f"Extração de Londres consolidada: {len(df_master):,} atletas no total."
        )
        return df_master