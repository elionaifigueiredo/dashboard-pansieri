import pandas as pd
import mysql.connector
import numpy as np

# ==============================
# CONEXÃO COM O BANCO
# ==============================

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="dashboard"
)

cursor = conn.cursor()

# ==============================
# LER PLANILHA (FORÇANDO TUDO COMO TEXTO)
# ==============================

df = pd.read_excel("base.xlsx", dtype=str)

# ==============================
# PADRONIZAR NOMES DAS COLUNAS
# ==============================

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_", regex=False)
)

# ==============================
# FUNÇÃO PARA CONVERTER DATAS
# ==============================

def converter_data(coluna):
    return (
        pd.to_datetime(coluna, dayfirst=True, errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

# ==============================
# CONVERTER COLUNAS DE DATA
# ==============================

datas = ["data_entrada", "data_agenda"]

for coluna in datas:
    if coluna in df.columns:
        df[coluna] = converter_data(df[coluna])

# ==============================
# CONVERTER NaN / NaT PARA NULL
# ==============================

df = df.replace({np.nan: None})
df = df.where(pd.notnull(df), None)

# ==============================
# GARANTIR TODAS AS COLUNAS
# ==============================

colunas = [
    "processo",
    "incidente",
    "data_entrada",
    "data_agenda",
    # "data_registro",
    "sigla",
    "integracao",
    "fase",
    "evento",
    "advogado",
    "autor",
    "reu",
    "grupo",
    "evento_pauta"
]

for col in colunas:
    if col not in df.columns:
        df[col] = None

df = df[colunas]

# ==============================
# DEBUG (opcional)
# ==============================

print("Visualização das datas convertidas:")
print(df[["data_entrada", "data_agenda"]].head())

# ==============================
# SQL INSERT
# ==============================

sql = """
INSERT INTO base (
    processo, incidente, data_entrada, data_agenda,
    sigla, integracao, fase, evento,
    advogado, autor, reu, grupo, evento_pauta
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# ==============================
# INSERÇÃO EM LOTE
# ==============================

dados = [tuple(row) for row in df.values]

try:
    cursor.executemany(sql, dados)
    conn.commit()
    print(f"{cursor.rowcount} registros inseridos com sucesso.")

except Exception as e:
    conn.rollback()
    print("Erro ao inserir:", e)

finally:
    cursor.close()
    conn.close()