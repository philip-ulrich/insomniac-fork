# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Expose port 5555 for ADB
EXPOSE 5555

# Run the bot when the container launches
CMD ["python", "run.py", "--config", "accounts/quecreate/config.yml", "--use-nocodb", "--debug"]
