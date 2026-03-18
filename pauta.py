import pandas as pd
import mysql.connector

# conexão
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="dashboard"
)

cursor = conn.cursor()

# ler planilha
df = pd.read_excel("pauta.xlsx")

# converter NaN para NULL
df = df.astype(object).where(pd.notnull(df), None)

# total e últimos 5
print("Total registros:", len(df))
print("\nÚltimos 5:")
print(df.tail(5))

sql = """
INSERT INTO arquivos
(pj,ficha,processo,origem,agenda_texto,evento,data_entrada,data_agendamento,
sigla,fase,evento_sigla,grupo,justificativa,trabalho_texto)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

for _, row in df.iterrows():
    cursor.execute(sql, tuple(row))

conn.commit()

print("\nInseridos:", cursor.rowcount)

cursor.close()
conn.close()