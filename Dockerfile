# Imagen base
FROM python:3

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de código fuente y los requisitos al contenedor
COPY .env .
COPY session_read.session .
COPY app.py .
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Ejecutar el código cuando se inicie el contenedor
CMD ["python", "app.py"]