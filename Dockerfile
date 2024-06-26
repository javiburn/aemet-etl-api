FROM debian:bullseye

# Actualiza el sistema e instala Python, pip, curl y gnupg
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl gnupg && \
    rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia los archivos requirements.txt y el resto del código al contenedor
COPY requirements.txt .
COPY . .

# Instala las dependencias de Python
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

RUN pip install --upgrade pyarrow

# Instala Node.js desde la fuente oficial
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y nodejs

# Instala Dataform globalmente desde npm
RUN npm install -g @dataform/core @dataform/cli

# Verifica la instalación de Python y Dataform
RUN python3 --version && dataform --version

# Establece la variable de entorno para las credenciales de Google Cloud
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/data-426015-2dbcae9be7fb.json"

# Establece el directorio de trabajo en el proyecto Dataform
WORKDIR /app/dataform

# Copia package.json antes de instalar Dataform
COPY dataform/package.json .

# Inicializa el proyecto Dataform (si es necesario)
RUN npm install

# Expose port 8080 (assuming your application runs on this port)
EXPOSE 8080

# Vuelve al directorio principal de la aplicación
WORKDIR /app

# Comando para iniciar el servidor Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]
