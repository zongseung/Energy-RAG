
# Use an official Python runtime as a parent image
FROM python:3.10-slim

ENV IS_DOCKERIZED=true

# Set the working directory in the container
WORKDIR /app

# Install uv
RUN pip install uv

# Copy the pyproject.toml and uv.lock files to the container
COPY pyproject.toml uv.lock /app/

# Install dependencies using uv
RUN uv pip install --system -r pyproject.toml

# Copy the rest of the application code to the container
COPY . /app

# Expose the Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app/app.py"]
