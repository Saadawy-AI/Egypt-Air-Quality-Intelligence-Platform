FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit==1.67.0 pandas==2.2.3 pyarrow==26.0.1 "psycopg[binary]==3.2.9"
COPY streamlit/app.py streamlit/Analysis_Dashboard.py streamlit/gold_data.py ./
COPY streamlit/pages ./pages
EXPOSE 8501
RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin app
ENV HOME=/home/app
USER 10001:10001
CMD ["streamlit", "run", "Analysis_Dashboard.py", "--server.address=0.0.0.0"]
