import pandas as pd
import mysql.connector
from datetime import datetime, time

# =========================
# CONFIGURAÇÕES DO BANCO
# =========================
conexao = mysql.connector.connect(
    host='localhost',
    user='root',
    password='1234',
    database='dashboard'
)
cursor = conexao.cursor(dictionary=True)

# =========================
# LER A PLANILHA
# =========================
df = pd.read_excel('dados.xlsx')

# Normaliza nomes das colunas
df.columns = df.columns.str.lower().str.strip()

# Converte data SEM timezone
df['data_entrada'] = pd.to_datetime(
    df['data_entrada'],
    errors='coerce',
    dayfirst=True
)

# Remove linhas inválidas
df = df.dropna(subset=['processo', 'sigla', 'data_entrada'])

# =========================
# INSERÇÃO NO MYSQL (COM HORA ATUAL)
# =========================
sql = """
INSERT INTO dados (processo, sigla, status, data_entrada)
VALUES (%s, %s, %s, %s)
"""

dados = []
for _, row in df.iterrows():
    # Combina data da planilha com hora atual
    data_planilha = row['data_entrada'].date()
    hora_atual = datetime.now().time()
    data_completa = datetime.combine(data_planilha, hora_atual)
    
    dados.append((
        str(row['processo']),
        row['sigla'],
        None,  # status
        data_completa
    ))

cursor.executemany(sql, dados)
conexao.commit()

# =========================
# BUSCAR O ÚLTIMO INSERT
# =========================
cursor.execute("""
    SELECT processo, sigla, status, data_entrada
    FROM dados
    ORDER BY id DESC
    LIMIT 1
""")

ultimo = cursor.fetchone()

cursor.close()
conexao.close()

# =========================
# PRINT FINAL
# =========================
print("✅ Dados inseridos com data da planilha + hora atual de inserção")
print("📌 Último registro inserido:")
print(f"   Processo     : {ultimo['processo']}")
print(f"   Sigla        : {ultimo['sigla']}")
print(f"   Status       : {ultimo['status']}")
print(f"   Data entrada : {ultimo['data_entrada']}")