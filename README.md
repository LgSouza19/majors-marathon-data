# World Marathon Majors Data & Analytics Pipeline 🏃‍♂️📊

Pipeline de Engenharia de Dados e Ciência do Esporte para ingestão, padronização e modelagem analítica das **6 World Marathon Majors** (Boston, Berlim, Chicago, Londres, Nova York e Tóquio), consolidando mais de **5,2 milhões de registros históricos de concluintes**.

---

## 🏆 Cobertura do Banco de Dados

| Major | Período Digital | Volume Concluído | Provedor / Tecnologia |
| :--- | :---: | :---: | :--- |
| 🏛️ **Boston Marathon** | 2001 – 2026 | ~600.000 | API REST Supabase (B.A.A.) |
| ⚡ **Berlin Marathon** | 1998 – 2025 | ~900.000 | API DataTables JSON (SCC Events) |
| 🏙️ **Chicago Marathon** | 1996 – 2025 | 996.890 | Multithreaded Crawler (Mika Timing) |
| 🇬🇧 **London Marathon** | 2001 – 2026 | 939.920 | Extrator Polimórfico (TCS / Mika) |
| 🗽 **New York City Marathon** | 1970 – 2025 | 1.518.077 | API RMS Prod JSON (NYRR) |
| 🗾 **Tokyo Marathon** | 2007 – 2026 | ~380.000 | Extrator Híbrido (Tokyo Foundation) |
| **TOTAL CONSOLIDADO** | **1970 – 2026** | **> 5.200.000** | **Schema Canônico Universal** |

---

## 🏗️ Arquitetura de Dados em Duas Camadas

O repositório adota a separação rigorosa entre dados brutos e dados analíticos processados:

1. **Camada Raw Lake (`data/raw/`):** 
   - Preservação integral de 100% das variáveis originais da fonte (até 70+ colunas por atleta).
   - Metadados detalhados: horas exatas de largada da onda, deltas de cada trecho, horários de passagem no tapete, clubes filiados, contagem histórica de provas (`racesCount`), idade exata contínua e percentual WMA de idade (`ageGradePercent`).
2. **Camada Processada Analítica (`data/processed/`):** 
   - Harmonização canônica global em unidades do Sistema Internacional (metros, segundos e minutos por quilômetro).
   - Matriz unificada pronta para regressões logísticas, modelos de sobrevivência (Survival Analysis) e algoritmos de Machine Learning.

---

## ⚙️ Variáveis Fisiológicas e Métricas Canônicas

1. **Parciais de 5 em 5 km:** `Time5K`, `Time10K`, `Time15K`, `Time20K`, `TimeHalf`, `Time25K`, `Time30K`, `Time35K`, `Time40K`, `ChipFinish`.
2. **Ritmos em Padrão Métrico Internacional:** Paces calculados matematicamente do zero a partir dos segundos e distâncias métricas:
   - **Pace do Segmento (min/km):** `(Delta_Tempo_Segundos / 5.0 km) / 60`
   - **Pace Geral da Prova (min/km):** `(ChipFinish_Segundos / 42.195 km) / 60`
3. **Métricas de Pacing e Quebra Fisiológica (*Hit the Wall*):**
   - **`Pacing_Ratio`:** `Tempo da 2ª Meia (s) / Tempo da 1ª Meia (s)`
   - **`Hit_The_Wall`:** Critério canônico da literatura (`Pacing_Ratio >= 1.20` — segunda metade $\ge 20\%$ mais lenta que a primeira).
   - **`Newton_Hills_Decay_Pct`:** Queda percentual de ritmo no trecho crítico das subidas (km 30–35 em Boston em relação à base inicial).
4. **Auditoria Ética e Integridade de Percurso (*Um Golpe Por Milha*):**
   - `Flag_Missing_Mats`: Atleta concluinte com perda de registro em tapetes intermediários em edições com infraestrutura ativa (distinguindo ausência tecnológica histórica de potencial corte de percurso).
   - `Flag_Pace_Impossivel`: Ritmos intermediários fisiologicamente inverossímeis para amadores (`Pace < 2:30 min/km` ou $\le 0$).
   - `Flag_Negative_Split_Extremo`: Segunda metade $\ge 25\%$ mais rápida que a primeira (`Pacing_Ratio < 0.75`).

---

## 📁 Estrutura do Repositório

```text
majors-marathon-data/
├── .gitignore                   # Configuração de arquivos e diretórios ignorados
├── README.md                    # Documentação científica e guia de execução
├── requirements.txt             # Dependências para instalação via pip
├── environment.yml              # Ambiente reproduzível do Conda
├── main.py                      # Orquestrador central da CLI
│
├── src/
│   ├── __init__.py
│   ├── config.py                # Endpoints, schemas e constantes fisiológicas
│   ├── utils.py                 # Funções vetoriais de conversão temporal
│   ├── transform.py             # Pipeline unificado de Fisiologia, Pacing e Auditoria
│   ├── consolidate_majors.py    # Consolidador mestre do dataset global
│   │
│   └── extractors/              # Módulos de extração individuais por Major
│       ├── __init__.py
│       ├── boston.py            # Boston Athletic Association (B.A.A.)
│       ├── berlin.py            # SCC Events / Mika Timing
│       ├── chicago.py           # Bank of America Chicago Marathon
│       ├── london.py            # TCS London Marathon
│       ├── tokyo.py             # Tokyo Marathon Foundation
│       └── ny.py                # New York Road Runners (NYRR)
│
├── notebooks/                   # Notebooks de exploração e modelagem científica
└── data/                        # Diretório de dados local (Raw Lake e Processed)
```

---

## 🚀 Como Executar

### 1. Configurar o Ambiente

Com o Conda (Recomendado):
```bash
conda env create -f environment.yml
conda activate marathon-majors
```

Ou com Pip / Virtualenv:
```bash
pip install -r requirements.txt
```

### 2. Executar a Extração de uma Major

A interface de linha de comando (`main.py`) orquestra a extração e a transformação de cada prova:

```bash
# Boston Marathon (2001 - 2026)
python main.py --race boston --start 2001 --end 2026

# Berlin Marathon (1998 - 2025)
python main.py --race berlin --start 1998 --end 2025

# Chicago Marathon (1996 - 2025)
python main.py --race chicago --start 1996 --end 2025

# London Marathon (2001 - 2026)
python main.py --race london --start 2001 --end 2026

# New York City Marathon (1970 - 2025)
python main.py --race ny --start 1970 --end 2025

# Tokyo Marathon (2007 - 2026)
python main.py --race tokyo --start 2007 --end 2026
```

### 3. Consolidar o Mega-Dataset Global

Para unir todas as provas processadas em um único arquivo Parquet consolidado:

```bash
python src/consolidate_majors.py
```

O comando gera o arquivo mestre: `data/processed/majors_master_consolidated.parquet`.

---

## 🔬 Pesquisa e Colaboração

Este repositório foi desenhado para subsidiar estudos na interseção entre **Big Data e Fisiologia do Exercício**, permitindo:
* Modelagem probabilística de colapso metabólico (*Wall Events*);
* Análise comparativa da influência da altimetria e condições ambientais sobre o ritmo de corrida;
* Rastreamento longitudinal de carreira e efeitos do envelhecimento sobre a performance (*Age-Graded Decline*);
* Auditoria estatística e integridade ética em eventos esportivos de massa.