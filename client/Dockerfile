FROM python

WORKDIR /client

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "client_script.py"]