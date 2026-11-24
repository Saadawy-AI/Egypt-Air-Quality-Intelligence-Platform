FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit pandas pyarrow "psycopg[binary]"
COPY app.py Analysis_Dashboard.py gold_data.py ./
COPY pages ./pages
EXPOSE 8501
CMD ["streamlit", "run", "Analysis_Dashboard.py", "--server.address=0.0.0.0"]
