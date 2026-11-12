FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data.py app.py ./

# Streamlit writes small settings files to HOME; give the unprivileged user a writable one.
ENV HOME=/tmp \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
USER nobody

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
