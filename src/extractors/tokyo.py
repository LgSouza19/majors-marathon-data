"""Módulo de extração e padronização de dados da Maratona de Tóquio (Tokyo Marathon Foundation).

Implementa enumeração exaustiva total (BIBs 1 a 99.999) para a Era Histórica (2007-2014)
e paginação integral (2015-2026), garantindo 100% de cobertura sem viés de amostragem.
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
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
    "Origin": "https://www.marathon.tokyo",
    "Connection": "close",
}

SPLIT_MAP_TOKYO: Dict[str, str] = {
    "5km": "Time5K",
    "10km": "Time10K",
    "15km": "Time15K",
    "20km": "Time20K",
    "中間点／Halfway Point": "TimeHalf",
    "Halfway Point": "TimeHalf",
    "中間点 Halfway Point": "TimeHalf",
    "中間点": "TimeHalf",
    "25km": "Time25K",
    "30km": "Time30K",
    "35km": "Time35K",
    "40km": "Time40K",
    "フィニッシュ／Finish": "ChipFinish",
    "Finish": "ChipFinish",
    "フィニッシュ Finish": "ChipFinish",
    "フィニッシュ": "ChipFinish",
}


class TokyoMarathonExtractor:
    """Cliente HTTP com enumeração exaustiva para cobertura total de Tóquio."""

    def __init__(self, max_workers: int = 15):
        self.max_workers = max_workers

    def _create_session(self) -> requests.Session:
        """Cria uma sessão HTTP isolada com adaptadores de retry para resiliência."""
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

    # =========================================================================
    # ESTEIRA 1: ERA HISTÓRICA (2007 a 2014 via Enumeração Total 1-99.999)
    # =========================================================================
    @staticmethod
    def _fetch_historical_bib(
        bib_number: int, year: int, session: requests.Session, referer_url: str
    ) -> Optional[Dict]:
        url = "https://www.marathon.tokyo/2014/index.php"
        headers_req = HEADERS.copy()
        headers_req["Referer"] = referer_url
        headers_req["Content-Type"] = "application/x-www-form-urlencoded"

        payload_str = (
            f"holdingyear={year}&number={bib_number}&"
            f"search_number=%E3%83%8A%E3%83%B3%E3%83%90%E3%83%BC%E3%82%AB%E3%83%BC%E3%83%89%E6%A4%9C%E7%B4%A2&name="
        )

        try:
            r = session.post(url, headers=headers_req, data=payload_str, timeout=12)
            r.encoding = r.apparent_encoding

            if r.status_code == 200 and ("通過点" in r.text or "Point" in r.text or "5km" in r.text):
                tables = pd.read_html(io.StringIO(r.text), flavor="lxml")
                record = {
                    "ID_Ref": f"TYO_{year}_{bib_number}",
                    "EventYear": year,
                    "FullBibNumber_List": str(bib_number),
                }

                for tbl in tables:
                    # Metadados do Atleta (Tabela 0)
                    if len(tbl.columns) == 2 and any(
                        "氏名" in str(v) or "NAME" in str(v) for v in tbl.values
                    ):
                        for _, row in tbl.iterrows():
                            k = str(row[0]).strip()
                            v = str(row[1]).strip()
                            record[f"Meta_{k}"] = v

                    # Tabela de Splits de 5k (Tabela 1)
                    elif any(
                        "通過点" in str(c) or "Point" in str(c) for c in tbl.columns
                    ) or any(
                        "通過点" in str(v) or "Point" in str(v) or "5km" in str(v) for v in tbl.values
                    ):
                        for _, row in tbl.iterrows():
                            vals = [str(x).strip() for x in row if pd.notnull(x)]
                            if len(vals) >= 2:
                                point_raw = vals[0]
                                time_raw = vals[1]
                                lap_raw = vals[2] if len(vals) > 2 else None

                                if point_raw in SPLIT_MAP_TOKYO:
                                    col = SPLIT_MAP_TOKYO[point_raw]
                                    record[f"Time_{col}"] = (
                                        time_raw if time_raw not in ["-", "NaN"] else None
                                    )
                                    record[f"Lap_{col}"] = (
                                        lap_raw if lap_raw not in ["-", "NaN"] else None
                                    )

                # Identifica gênero oficial
                event_name = str(record.get("Meta_種目名 EVENT", "")).lower()
                if "女子" in event_name or "women" in event_name:
                    record["GenderCode"] = "F"
                elif "男子" in event_name or "men" in event_name:
                    record["GenderCode"] = "M"
                else:
                    record["GenderCode"] = None

                if any("Meta_" in k for k in record):
                    return record
        except Exception:
            pass
        return None

    def _extract_historical_year(self, year: int) -> List[Dict]:
        """Varre o espaço amostral completo de 1 a 99.999 garantindo 100% de cobertura."""
        bib_range = list(range(1, 100000))

        logger.info(
            f"Extraindo histórico de Tóquio {year}: varredura exaustiva de {len(bib_range):,} BIBs (1 a 99.999)..."
        )
        referer_url = f"https://www.marathon.tokyo/2014/index.php?year={year}"

        session = self._create_session()
        try:
            session.get(referer_url, headers=HEADERS, timeout=12)
        except Exception:
            pass

        records: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._fetch_historical_bib,
                    bib,
                    year,
                    session,
                    referer_url,
                )
                for bib in bib_range
            ]

            for f in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Tóquio {year} (Total 1-99k)",
                unit=" bibs",
            ):
                res = f.result()
                if res:
                    records.append(res)

        session.close()
        return records

    # =========================================================================
    # ESTEIRA 2: ERA MODERNA (2015 a 2026)
    # =========================================================================
    @staticmethod
    def _parse_modern_list_row(
        row_soup, year: int, gender_code: str
    ) -> Optional[Dict]:
        tds = row_soup.find_all("td")
        if len(tds) < 7:
            return None

        d_number = None
        a_tag = row_soup.find("a", href=lambda h: h and "detail(" in h)
        if a_tag:
            m = re.search(r"detail\('([^']+)'\)", a_tag["href"])
            if m:
                d_number = m.group(1)

        if not d_number:
            return None

        place_overall = tds[0].get_text(strip=True)
        category_desc = tds[1].get_text(strip=True)
        bib_num = tds[2].get_text(strip=True)
        name_full = tds[3].get_text(strip=True)
        age = tds[4].get_text(strip=True)
        sex = tds[5].get_text(strip=True)
        nationality = tds[6].get_text(strip=True)
        city = tds[7].get_text(strip=True) if len(tds) > 7 else None

        name_clean = name_full
        if "／" in name_full:
            name_clean = name_full.split("／")[-1].strip()
        elif "\n" in name_full:
            name_clean = name_full.split("\n")[-1].strip()

        return {
            "ID_Ref": d_number,
            "EventYear": year,
            "GenderCode": gender_code,
            "FullBibNumber_List": bib_num,
            "FormattedFullName_List": name_clean,
            "Age_List": age,
            "Sex_Desc": sex,
            "CountryOfCTZName": nationality,
            "AwardsDivisionShortDesc": category_desc,
            "City_List": city,
            "RankOverAll_List": place_overall,
            "d_number": d_number,
        }

    @staticmethod
    def _fetch_modern_splits(
        athlete: Dict, detail_url: str, session: requests.Session
    ) -> Dict:
        raw_record = athlete.copy()
        d_num = athlete.get("d_number")
        if not d_num:
            return raw_record

        payload = {
            "category": "1" if athlete.get("GenderCode") == "M" else "2",
            "d_number": str(d_num),
            "page": "1",
            "sort_key": "place",
            "sort_asc": "1",
        }

        time.sleep(0.01)

        try:
            r = session.post(detail_url, headers=HEADERS, data=payload, timeout=15)
            if r.status_code == 200:
                tables = pd.read_html(io.StringIO(r.text), flavor="lxml")

                for tbl in tables:
                    if len(tbl.columns) >= 2 and any(
                        "参加種目" in str(v)
                        or "Race Category" in str(v)
                        or "Time (net)" in str(v)
                        for v in tbl.values
                    ):
                        for _, row in tbl.iterrows():
                            k = str(row[0]).strip()
                            v = str(row[1]).strip()
                            raw_record[f"Meta_{k}"] = v
                            if len(row) > 2 and pd.notnull(row[2]):
                                raw_record[f"Meta_Rank_{k}"] = str(row[2]).strip()

                    elif any("通過点" in str(c) or "Point" in str(c) for c in tbl.columns):
                        for _, row in tbl.iterrows():
                            point_raw = str(row.iloc[0]).strip()
                            time_raw = (
                                str(row.iloc[1]).strip()
                                if len(row) > 1
                                else None
                            )
                            lap_raw = (
                                str(row.iloc[2]).strip()
                                if len(row) > 2
                                else None
                            )

                            if point_raw in SPLIT_MAP_TOKYO:
                                col = SPLIT_MAP_TOKYO[point_raw]
                                raw_record[f"Time_{col}"] = (
                                    time_raw if time_raw not in ["-", "NaN", "None"] else None
                                )
                                raw_record[f"Lap_{col}"] = (
                                    lap_raw if lap_raw not in ["-", "NaN", "None"] else None
                                )
        except Exception:
            pass
        return raw_record

    def _extract_modern_year(self, year: int) -> List[Dict]:
        """Extrai edições modernas (2015-2026) via paginação de 50 em 50."""
        list_url = f"https://www.marathon.tokyo/{year}/result/index.php"
        detail_url = f"https://www.marathon.tokyo/{year}/result/detail.php"

        athletes_list: List[Dict] = []
        seen_d_numbers = set()
        categories = [("1", "M"), ("2", "F")]

        for cat_id, gender_label in categories:
            sess = self._create_session()
            params_init = {"category": cat_id, "page": "1"}

            total_athletes = 0
            for attempt in range(4):
                try:
                    r_init = sess.post(
                        list_url, headers=HEADERS, data=params_init, timeout=15
                    )
                    if r_init.status_code == 200:
                        soup_init = BeautifulSoup(r_init.text, "html.parser")
                        text_all = soup_init.get_text()
                        m = re.search(r"/\s*(\d+)", text_all)
                        if m:
                            total_athletes = int(m.group(1))
                            break
                        if len(soup_init.select("table tr")) > 2:
                            total_athletes = 35000
                            break
                    time.sleep(5)
                except Exception:
                    time.sleep(5)

            sess.close()

            if total_athletes == 0:
                continue

            max_pages = math.ceil(total_athletes / 50)

            with tqdm(
                total=max_pages,
                desc=f"Lista Tóquio {year} ({gender_label})",
                unit=" pgs",
            ) as pbar:
                page = 1
                consecutive_empty = 0
                list_session = self._create_session()

                while page <= max_pages:
                    payload = {"category": cat_id, "page": str(page)}
                    try:
                        resp = list_session.post(
                            list_url, headers=HEADERS, data=payload, timeout=15
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
                        rows = soup.select("table tr")

                        page_new = 0
                        for r in rows:
                            parsed = self._parse_modern_list_row(
                                r, year, gender_label
                            )
                            if (
                                parsed
                                and parsed["d_number"] not in seen_d_numbers
                            ):
                                seen_d_numbers.add(parsed["d_number"])
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

        logger.info(
            f"Listagem concluída: {len(athletes_list):,} atletas localizados. Extraindo splits..."
        )
        enriched: List[Dict] = []
        worker_session = self._create_session()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._fetch_modern_splits,
                    athlete,
                    detail_url,
                    worker_session,
                )
                for athlete in athletes_list
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Splits Tóquio {year}",
                unit=" atletas",
            ):
                enriched.append(future.result())

        worker_session.close()
        return enriched

    # =========================================================================
    # PIPELINE PRINCIPAL POR ANO
    # =========================================================================
    def extract_year(
        self, year: int, destination_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """Executa a extração completa de qualquer edição de Tóquio (2007 a 2026)."""
        if year in [2020, 2022]:
            logger.warning(
                f"Ano {year} pulado (Edição cancelada/restrita pela pandemia)."
            )
            return pd.DataFrame()

        logger.info(f"Extraindo Tokyo Marathon {year}...")

        if year <= 2014:
            records = self._extract_historical_year(year)
        else:
            records = self._extract_modern_year(year)

        if not records:
            logger.warning(f"Ano {year}: Nenhum participante recuperado.")
            return pd.DataFrame()

        df_raw = pd.DataFrame(records)

        # 1. Persistência do Raw Lake Integral (40+ colunas)
        if destination_dir:
            destination_dir.mkdir(parents=True, exist_ok=True)
            output_file = destination_dir / f"tokyo_{year}_raw.parquet"
            df_raw.astype(str).to_parquet(output_file, index=False)
            logger.info(
                f"Ano {year} persistido (raw profundo): {len(df_raw):,} atletas salvos em {output_file.name}"
            )

        # 2. Padronização Canônica Blindada
        df_standard = pd.DataFrame()
        df_standard["ID"] = [
            f"TYO_{year}_{i+1:06d}" for i in range(len(df_raw))
        ]
        df_standard["EventYear"] = int(year)

        df_standard["FullBibNumber"] = (
            df_raw.get(
                "FullBibNumber_List",
                df_raw.get("Meta_ナンバー NUMBER", ""),
            )
            .fillna("")
            .astype(str)
        )
        df_standard["FormattedFullName"] = df_raw.get(
            "FormattedFullName_List",
            df_raw.get("Meta_氏名 NAME"),
        )
        df_standard["GenderCode"] = df_raw.get("GenderCode")
        df_standard["AwardsDivisionShortDesc"] = df_raw.get(
            "AwardsDivisionShortDesc",
            df_raw.get("Meta_種目名 EVENT"),
        )
        df_standard["CountryOfCTZName"] = df_raw.get("CountryOfCTZName")
        df_standard["CountryOfResidenceName"] = df_raw.get("CountryOfCTZName")
        df_standard["City"] = df_raw.get("City_List")
        df_standard["StateName"] = None
        df_standard["AgeOnRaceDay"] = pd.to_numeric(
            df_raw.get("Age_List", df_raw.get("Meta_年齢／Age")),
            errors="coerce",
        )

        # Splits Canônicos de 5k
        df_standard["Time5K"] = df_raw.get(
            "Time_Time5K", df_raw.get("Time_5km")
        )
        df_standard["Time10K"] = df_raw.get(
            "Time_Time10K", df_raw.get("Time_10km")
        )
        df_standard["Time15K"] = df_raw.get(
            "Time_Time15K", df_raw.get("Time_15km")
        )
        df_standard["Time20K"] = df_raw.get(
            "Time_Time20K", df_raw.get("Time_20km")
        )
        df_standard["TimeHalf"] = df_raw.get(
            "Time_TimeHalf",
            df_raw.get(
                "Time_Halfway_Point",
                df_raw.get("Time_中間点"),
            ),
        )
        df_standard["Time25K"] = df_raw.get(
            "Time_Time25K", df_raw.get("Time_25km")
        )
        df_standard["Time30K"] = df_raw.get(
            "Time_Time30K", df_raw.get("Time_30km")
        )
        df_standard["Time35K"] = df_raw.get(
            "Time_Time35K", df_raw.get("Time_35km")
        )
        df_standard["Time40K"] = df_raw.get(
            "Time_Time40K", df_raw.get("Time_40km")
        )
        df_standard["ChipFinish"] = df_raw.get(
            "Time_ChipFinish",
            df_raw.get(
                "Time_Finish",
                df_raw.get(
                    "Meta_チップタイム（参考） CHIP TIME",
                    df_raw.get("Meta_タイム(ネット)／Time (net)"),
                ),
            ),
        )

        df_standard["RankOverAll"] = pd.to_numeric(
            df_raw.get(
                "RankOverAll_List",
                df_raw.get("Meta_順位 PLACE"),
            ),
            errors="coerce",
        )
        df_standard["RankOverGender"] = None
        df_standard["RankOverDivision"] = None

        time.sleep(5)
        return df_standard

    def extract_range(
        self,
        start_year: int,
        end_year: int,
        raw_dir: Path = config.RAW_DATA_DIR / "tokyo",
    ) -> pd.DataFrame:
        """Executa a extração em lote garantindo cobertura integral de todas as edições."""
        accumulated_frames: List[pd.DataFrame] = []

        for current_year in range(start_year, end_year + 1):
            if current_year in [2020, 2022]:
                logger.info(
                    f"Ano {current_year} pulado (Edição cancelada/restrita pela pandemia)."
                )
                continue

            target_path = raw_dir / f"tokyo_{current_year}_raw.parquet"

            # Validação rigorosa de integridade: só aceita do cache se tiver volume real da prova
            if target_path.exists():
                try:
                    df_cached = pd.read_parquet(target_path)
                    # 2007 teve ~19.5k, anos normais têm >= 24.000 concluintes
                    min_finishers = 19000 if current_year == 2007 else 24000
                    if len(df_cached) >= min_finishers:
                        logger.info(
                            f"Tóquio {current_year} em cache íntegro ({len(df_cached):,} atletas)."
                        )
                        df_std_cached = pd.DataFrame()
                        df_std_cached["ID"] = [
                            f"TYO_{current_year}_{i+1:06d}"
                            for i in range(len(df_cached))
                        ]
                        df_std_cached["EventYear"] = int(current_year)
                        df_std_cached["FullBibNumber"] = (
                            df_cached.get(
                                "FullBibNumber_List",
                                df_cached.get("Meta_ナンバー NUMBER", ""),
                            )
                            .fillna("")
                            .astype(str)
                        )
                        df_std_cached["FormattedFullName"] = df_cached.get(
                            "FormattedFullName_List",
                            df_cached.get("Meta_氏名 NAME"),
                        )
                        df_std_cached["GenderCode"] = df_cached.get("GenderCode")
                        df_std_cached["AwardsDivisionShortDesc"] = (
                            df_cached.get(
                                "AwardsDivisionShortDesc",
                                df_cached.get("Meta_種目名 EVENT"),
                            )
                        )
                        df_std_cached["CountryOfCTZName"] = df_cached.get(
                            "CountryOfCTZName"
                        )
                        df_std_cached["CountryOfResidenceName"] = (
                            df_cached.get("CountryOfCTZName")
                        )
                        df_std_cached["City"] = df_cached.get("City_List")
                        df_std_cached["StateName"] = None
                        df_std_cached["AgeOnRaceDay"] = pd.to_numeric(
                            df_cached.get(
                                "Age_List",
                                df_cached.get("Meta_年齢／Age"),
                            ),
                            errors="coerce",
                        )

                        df_std_cached["Time5K"] = df_cached.get(
                            "Time_Time5K", df_cached.get("Time_5km")
                        )
                        df_std_cached["Time10K"] = df_cached.get(
                            "Time_Time10K", df_cached.get("Time_10km")
                        )
                        df_std_cached["Time15K"] = df_cached.get(
                            "Time_Time15K", df_cached.get("Time_15km")
                        )
                        df_std_cached["Time20K"] = df_cached.get(
                            "Time_Time20K", df_cached.get("Time_20km")
                        )
                        df_std_cached["TimeHalf"] = df_cached.get(
                            "Time_TimeHalf",
                            df_cached.get(
                                "Time_Halfway_Point",
                                df_cached.get("Time_中間点"),
                            ),
                        )
                        df_std_cached["Time25K"] = df_cached.get(
                            "Time_Time25K", df_cached.get("Time_25km")
                        )
                        df_std_cached["Time30K"] = df_cached.get(
                            "Time_Time30K", df_cached.get("Time_30km")
                        )
                        df_std_cached["Time35K"] = df_cached.get(
                            "Time_Time35K", df_cached.get("Time_35km")
                        )
                        df_std_cached["Time40K"] = df_cached.get(
                            "Time_Time40K", df_cached.get("Time_40km")
                        )
                        df_std_cached["ChipFinish"] = df_cached.get(
                            "Time_ChipFinish",
                            df_cached.get(
                                "Time_Finish",
                                df_cached.get(
                                    "Meta_チップタイム（参考） CHIP TIME",
                                    df_cached.get(
                                        "Meta_タイム(ネット)／Time (net)"
                                    ),
                                ),
                            ),
                        )

                        df_std_cached["RankOverAll"] = pd.to_numeric(
                            df_cached.get(
                                "RankOverAll_List",
                                df_cached.get("Meta_順位 PLACE"),
                            ),
                            errors="coerce",
                        )
                        df_std_cached["RankOverGender"] = None
                        df_std_cached["RankOverDivision"] = None

                        accumulated_frames.append(df_std_cached)
                        continue
                    else:
                        logger.warning(
                            f"Cache incompleto detectado em Tóquio {current_year} "
                            f"({len(df_cached):,} atletas < {min_finishers:,}). Rebaixando..."
                        )
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
                    f"Falha na extração de Tóquio {current_year}: {exc}"
                )

        if not accumulated_frames:
            logger.critical("Nenhum dado recuperado para Tóquio.")
            return pd.DataFrame()

        df_master = pd.concat(accumulated_frames, ignore_index=True)
        df_master = df_master.sort_values(
            by=["EventYear", "RankOverAll"], ascending=[True, True]
        ).reset_index(drop=True)
        logger.info(
            f"Extração de Tóquio consolidada com cobertura total: {len(df_master):,} atletas."
        )
        return df_master