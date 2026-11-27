FROM apache/spark:3.5.7

USER root
# Ship Spark application code in the image instead of mounting it from Windows.
COPY --chown=185:185 spark /opt/spark-apps
USER 185:185
