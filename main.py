"""Orquestrador central do pipeline de dados para as World Marathon Majors.

Permite a extração e o processamento de Boston, Berlim, Chicago e Londres.
"""

import argparse
import logging
import sys
from pathlib import Path
import pandas as pd

from src import config
from src.extractors.berlin import BerlinMarathonExtractor
from src.extractors.boston import BostonMarathonExtractor
from src.extractors.chicago import ChicagoMarathonExtractor
from src.extractors.london import LondonMarathonExtractor
from src.extractors.tokyo import TokyoMarathonExtractor
from src.extractors.ny import NewYorkMarathonExtractor
from src.transform import MarathonTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("marathon_pipeline")

# Mapeamento de extratores ativos
EXTRACTORS = {
    "boston": BostonMarathonExtractor,
    "berlin": BerlinMarathonExtractor,
    "chicago": ChicagoMarathonExtractor,
    "london": LondonMarathonExtractor,
    "tokyo": TokyoMarathonExtractor,   # Próximas frentes
    "ny": NewYorkMarathonExtractor,
}


def run_pipeline(
    race: str,
    start_year: int,
    end_year: int,
    skip_extraction: bool = False,
    require_all_splits: bool = True,
) -> None:
    """Executa a extração e a transformação para uma maratona específica."""
    race_key = race.lower().strip()
    if race_key not in EXTRACTORS:
        logger.error(
            f"Prova '{race}' não suportada. Opções válidas: {list(EXTRACTORS.keys())}"
        )
        sys.exit(1)

    logger.info("=" * 70)
    logger.info(f"INICIANDO PIPELINE: {race_key.upper()} ({start_year} - {end_year})")
    logger.info("=" * 70)

    raw_race_dir = config.RAW_DATA_DIR / race_key
    processed_race_dir = config.PROCESSED_DATA_DIR / race_key
    raw_race_dir.mkdir(parents=True, exist_ok=True)
    processed_race_dir.mkdir(parents=True, exist_ok=True)

    # 1. EXTRAÇÃO (Ingestão do Raw Lake)
    raw_master_file = raw_race_dir / f"{race_key}_master_raw_{start_year}_{end_year}.parquet"

    if not skip_extraction:
        extractor_cls = EXTRACTORS[race_key]
        extractor = extractor_cls()
        df_raw = extractor.extract_range(start_year, end_year, raw_dir=raw_race_dir)

        if df_raw.empty:
            logger.error("Pipeline interrompido: nenhum dado bruto obtido.")
            sys.exit(1)

        df_raw.to_parquet(raw_master_file, index=False)
        logger.info(f"Arquivo bruto consolidado salvo: {raw_master_file}")
    else:
        if not raw_master_file.exists():
            logger.error(f"Arquivo de cache bruto não encontrado: {raw_master_file}")
            sys.exit(1)
        logger.info(f"Carregando dados brutos do cache: {raw_master_file}")
        df_raw = pd.read_parquet(raw_master_file)

    # 2. TRANSFORMAÇÃO (Pacing, Fisiologia e Filtragem de Pureza)
    transformer = MarathonTransformer()
    df_processed = transformer.transform(
        df_raw, race_name=race_key.capitalize()
    )

    # 3. PERSISTÊNCIA DOS DADOS PROCESSADOS
    processed_master_file = (
        processed_race_dir / f"{race_key}_master_processed_{start_year}_{end_year}.parquet"
    )
    df_processed.to_parquet(processed_master_file, index=False)

    logger.info("=" * 70)
    logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
    logger.info(f"Prova: {race_key.upper()}")
    logger.info(f"Total de atletas analisáveis: {len(df_processed):,}")
    logger.info(f"Dimensões finais: {df_processed.shape[0]:,} linhas x {df_processed.shape[1]} colunas")
    logger.info(f"Dataset salvo em: {processed_master_file}")
    logger.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline ETL das World Marathon Majors (Endurance & Data Science)"
    )
    parser.add_argument(
        "--race",
        type=str,
        default="boston",
        choices=["boston", "berlin", "chicago", "london", "tokyo", "ny"],
        help="Identificador da Major a ser processada",
    )
    parser.add_argument(
        "--start", type=int, default=2001, help="Ano inicial da extração"
    )
    parser.add_argument(
        "--end", type=int, default=2026, help="Ano final da extração"
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Pula a extração e reprocessa a partir dos arquivos brutos em disco",
    )


    args = parser.parse_args()
    run_pipeline(
        race=args.race,
        start_year=args.start,
        end_year=args.end,
        skip_extraction=args.skip_extract,
    )