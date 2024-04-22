# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Install necessary system packages
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    libmariadb-dev  # or libmysqlclient-dev

# Set the working directory to /app
WORKDIR /app

# Install any needed packages specified in requirements.txt
# (do this after copying the requirements file to use Docker's layer cache)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# Run app.py when the container launches
CMD ["python", "main.py"]
