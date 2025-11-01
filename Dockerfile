# Use the latest Python slim image
FROM python:3.13-slim

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary system packages for ODBC Driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    locales \
    # Remove any conflicting older versions if present (optional but safe)
    # && apt-get remove -y msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (using updated key addition method)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/22.04/prod/ jammy main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    # Clean up apt cache to keep image size small
    && rm -rf /var/lib/apt/lists/*

# Set up system locale
RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Set the working directory to /app
WORKDIR /app

# Create logs directory to avoid FileNotFoundError in logging
RUN mkdir -p /app/logs

# Copy requirements and install. Ensure pyodbc is in requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code into the container
COPY . .

# Set the command to run your scheduler script
CMD ["python", "main_scheduler.py"]