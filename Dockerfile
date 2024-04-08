FROM python:3.12

# Copiar o código-fonte para o contêiner
COPY . /src

# Definir o diretório de trabalho
WORKDIR /src

# Instalar as dependências do projeto a partir do requirements.txt
RUN pip install -r requirements.txt

# Expor a porta 8501
EXPOSE 8501

# Configurar o comando de entrada para iniciar o Streamlit
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
