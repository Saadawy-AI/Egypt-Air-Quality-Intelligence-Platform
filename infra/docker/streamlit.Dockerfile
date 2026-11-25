FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit pandas pyarrow "psycopg[binary]"
COPY streamlit/app.py streamlit/Analysis_Dashboard.py streamlit/gold_data.py ./
COPY streamlit/pages ./pages
EXPOSE 8501
CMD ["streamlit", "run", "Analysis_Dashboard.py", "--server.address=0.0.0.0"]
