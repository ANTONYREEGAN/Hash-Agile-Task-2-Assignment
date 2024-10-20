# Flask Application with dependencies installed manually
FROM python:3.9-slim

WORKDIR /app

# Install the required Python dependencies
RUN pip install elasticsearch pandas flask

# Copy the app files into the container
COPY . .

EXPOSE 5000

# Command to run the Flask app
CMD ["python", "app.py"]
