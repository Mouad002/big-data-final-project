# Use official Spark image (matches Hadoop 3.x)
FROM apache/spark:latest

# Switch to root to install Python packages
USER root

# Install Python deps if needed (usually not required for PySpark)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the spark user (or whatever non-root user the image uses)
USER spark

# Copy your Spark application
COPY traffic_processor.py /opt/spark-apps/traffic_processor.py

# Set entrypoint to run the job (you can also trigger manually)
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--name", "TrafficProcessingJob", \
     "/opt/spark-apps/traffic_processor.py"]

# COPY run_periodic.sh /opt/spark-apps/run_periodic.sh
# RUN chmod +x /opt/spark-apps/run_periodic.sh
# CMD ["/opt/spark-apps/run_periodic.sh"]