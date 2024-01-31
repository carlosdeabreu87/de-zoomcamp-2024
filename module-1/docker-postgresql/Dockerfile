FROM python:3.9

# Install wget
RUN apt-get update && apt-get install -y wget

# Set the working directory to /app
WORKDIR /app

# Copy the requirements.txt file to the container at /app
COPY requirements.txt requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Copy the rest of your application's code
COPY upload-data.py upload-data.py

# Define the command to run your application
ENTRYPOINT ["python", "upload-data.py"]
