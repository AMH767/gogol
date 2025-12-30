# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install system dependencies for Chrome and Selenium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    libnss3 \
    libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/var/lib/data

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories and set permissions
RUN mkdir -p static/screenshots templates && chmod -R 777 /app

# Render.com will use the PORT environment variable if provided, 
# but we can default to 10000 or keep 7860. 
# Let's use 10000 as it's standard for Render.
EXPOSE 10000

# Run the application with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--workers", "1", "--timeout", "300", "app_google:app"]
