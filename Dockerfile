FROM python:3.10-slim

# Set environment variables

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1


# Set the working directory
WORKDIR /app

# Install python requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run the application
CMD ["python", "main.py"]