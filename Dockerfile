FROM apache/spark:3.4.1
USER root

# Instala Python e pip, se necessário
RUN apt-get update && apt-get install -y python3 python3-pip

# Configura diretório de trabalho
WORKDIR /app

# Copia o código e as dependências
COPY app /app
COPY requirements.txt /app
COPY tests /app/tests
COPY .coveragerc /app

# Instala dependências do Python
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Configura o ponto de entrada
ENTRYPOINT ["python3", "main.py"]
