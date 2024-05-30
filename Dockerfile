# Usar uma imagem oficial do Python como base
FROM python:3.9-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Criar e definir o diretório de trabalho
WORKDIR /app

# Copiar arquivos de requisitos e instalar dependências do Python
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código
COPY . .

# Expor a porta que o Prefect usará (opcional)
EXPOSE 4200

# Comando padrão para rodar Prefect
CMD ["python", "run.py"]
