FROM python:3.11.4

LABEL Author="MTA"
LABEL version="0.2.0"

# Install uv
RUN pip install --upgrade pip && pip install uv

# Set working directory
WORKDIR /app

# Copy only the dependency files first (for cache efficiency)
COPY pyproject.toml uv.lock /app/

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Copy the rest of the application code
COPY . /app

# Specify the entry point
ENTRYPOINT ["uv", "run", "python", "-m", "app.main"]