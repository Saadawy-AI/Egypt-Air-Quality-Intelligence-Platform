FROM python:3.11-alpine
WORKDIR /app
COPY app.py /app/app.py
USER 10001:10001
EXPOSE 8088
CMD ["python","-u","/app/app.py"]
