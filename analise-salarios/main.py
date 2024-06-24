import psycopg2
import csv
import pandas as pd
#Bibliotecas adicionais
import matplotlib.pyplot as plt
import seaborn as sns

connection = psycopg2.connect(database="salary_2024", 
                              host="localhost", 
                              user="postgres", 
                              password="admin",
                              port="5432",
                              options='-c client_encoding=UTF8')
print(connection.info)
print(connection.status)

#Abrir cursos para operações do banco de dados
cur = connection.cursor()

# Criando a tabela 
cur.execute("""CREATE TABLE IF NOT EXISTS users(
	id SERIAL PRIMARY KEY, 
	work_year DATE, 
	experience_level VARCHAR(3),
	employment_type VARCHAR(3), 
	job_title VARCHAR(55),
	salary DECIMAL(10, 2),
	salary_currency VARCHAR(5),
	salary_in_usd DECIMAL(10, 2),
	employee_residence VARCHAR(55),
	remote_ratio INTEGER, 
	company_location VARCHAR(10),
	company_size VARCHAR(3));
            """)

# Abrir o arquivo CSV
with open('analise-salarios/dataset_salary_2024.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Ajustar a data para o formato correto (YYYY-MM-DD)
        work_year = f"{row['work_year']}-01-01"

        # Inserir cada linha no CSV na tabela 
        cur.execute("""
            INSERT INTO users (
                    work_year, experience_level, employment_type, job_title, salary, salary_currency, salary_in_usd, employee_residence, 
                    remote_ratio, company_location, company_size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,(
                        work_year, row['experience_level'], row['employment_type'],
                        row['job_title'], row['salary'], row['salary_currency'], row['salary_in_usd'],
                        row['employee_residence'], row['remote_ratio'], row['company_location'], row['company_size']
                    ))


#Fazer alterações no banco de dados persistente
connection.commit()


# Carregar dados para um DataFrame ddo Pandas
query = "SELECT * FROM users"
df = pd.read_sql_query(query, connection)

#Visualizar Dados
#print(df.head())
#print(df.info())
#print(df.describe())

# Estatísticas descritivas adicionais para colunas específicas
print("Média de salário:", round(df['salary'].mean(), 2))
print("Mediana de salário:", round(df['salary'].median(), 2))
print("Desvio padrão de salário:", round(df['salary'].std(), 2))
print("Valor mínimo de salário:", round(df['salary'].min(), 2))
print("Valor máximo de salário:", round(df['salary'].max(), 2))
print("Primeiro quartil de salário:", round(df['salary'].quantile(0.25), 2))
print("Terceiro quartil de salário:", round(df['salary'].quantile(0.75), 2))
print("Moda de salário:", round(df['salary'].mode()[0], 2))

print('-------')

# Distribuição de frequências 
print(df['job_title'].value_counts())

# Ajustar o formato da coluna work_year para datetime
df['work_year'] = pd.to_datetime(df['work_year'])

# Visualização com gráficos de linha
plt.figure(figsize=(14, 7))
sns.lineplot(x='work_year', y='salary', data=df, marker='o')
plt.title('Salário ao Longo dos Anos')
plt.xlabel('Ano de Trabalho')
plt.ylabel('Salário')
plt.grid(True)
plt.show()

plt.figure(figsize=(14, 7))
sns.lineplot(x='work_year', y='salary_in_usd', data=df, marker='o', color='r')
plt.title('Salário em USD ao Longo dos Anos')
plt.xlabel('Ano de Trabalho')
plt.ylabel('Salário em USD')
plt.grid(True)
plt.show()

plt.figure(figsize=(14, 7))
sns.lineplot(x='work_year', y='remote_ratio', data=df, marker='o', color='g')
plt.title('Proporção de Trabalho Remoto ao Longo dos Anos')
plt.xlabel('Ano de Trabalho')
plt.ylabel('Proporção de Trabalho Remoto')
plt.grid(True)
plt.show()

# Gráficos de linha por categoria
plt.figure(figsize=(14, 7))
sns.lineplot(x='work_year', y='salary', hue='experience_level', data=df, marker='o')
plt.title('Salário ao Longo dos Anos por Nível de Experiência')
plt.xlabel('Ano de Trabalho')
plt.ylabel('Salário')
plt.grid(True)
plt.legend(title='Nível de Experiência')
plt.show()

plt.figure(figsize=(14, 7))
sns.lineplot(x='work_year', y='salary', hue='employment_type', data=df, marker='o')
plt.title('Salário ao Longo dos Anos por Tipo de Emprego')
plt.xlabel('Ano de Trabalho')
plt.ylabel('Salário')
plt.grid(True)
plt.legend(title='Tipo de Emprego')
plt.show()
#Fecha o cursor com a comunicação com banco de dados 
cur.close()
connection.close()