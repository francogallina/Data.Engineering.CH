import requests
import pandas as pd
import time
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import json
from datetime import datetime
import psycopg2
import smtplib
from email.mime.text import MIMEText


"""----------------------------------EXTRACCIÓN DATOS DE LA API----------------------------------"""

def get_stock_data(**context):
  list_symbols = ['AAPL', 'MSFT', 'JPM', 'V', 'UNH', 'JNJ', 'WMT', 'HD', 'PG', 'DIS', 'KO', 'CSCO', 'CVX', 'NKE', 'CRM', 'INTC', 'VZ', 'MRK', 'MCD', 'HON', 'BA', 'AMGN', 'AXP', 'GS', 'CAT', 'IBM', 'MMM', 'WBA', 'DOW', 'TRV']

  URL_API= "https://api.twelvedata.com/time_series"

  list_prices = []
  
  for symbol in list_symbols:
    parameters = {
      "symbol": symbol,
      "interval": "1day",
      "format": "JSON",
      "start_date": "05/01/2023 6:36 PM",
      "end_date": datetime.now().strftime("%m/%d/%Y %I:%M %p"),
      "apikey": context["var"]["value"].get("APIKEY_TWELVEDATA")}

    response = requests.get(URL_API,parameters)
    if response.status_code == 200:
      data=response.json()
      if "meta" in data:
        list_prices.append(data)
      else:
        time.sleep(10)

  stock_prices = pd.json_normalize(list_prices, 'values', [['meta','symbol']])
  stock_prices = stock_prices.reindex(["meta.symbol", "datetime", "open", "high", "low", "close", "volume"],axis=1)
  stock_prices.columns = ["symbol", "date", "open_price", "high_price", "low_price", "close_price", "volume"]
  
  stock_prices.to_csv("stock_prices.csv", index=False)
  print("Archivo creado")

"""----------------------------------CONEXIÓN Y CARGA A BASE DE DATOS----------------------------------"""
def conexion_redshift(**context):
  host_db = context["var"]["value"].get("HOST_DB")
  database_db = context["var"]["value"].get("DATABASE_NAME")
  username_db = context["var"]["value"].get("USERNAME_DB")
  password_db = context["var"]["value"].get("PASSWORD_DB")
  
  try:
    conn = psycopg2.connect(
      host= host_db,
      dbname = database_db,
      user=username_db,
      password=password_db,
      port='5439')
    print(conn)
    print("Connected to Redshift successfully!")
  except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)

  engine = create_engine(f"postgresql://{username_db}:{password_db}@{host_db}:5439/{database_db}")

  stock_prices=pd.read_csv("stock_prices.csv")
  stock_prices.to_sql('stock_prices', engine, schema="franco_049_coderhouse", if_exists='replace', index=False)


"""----------------------------------ENVÍO DE EMAIL----------------------------------"""
def send_email(**context):
  day = datetime.now().strftime("%m/%d/%Y %I:%M %p"),
  subject = "Carga de datos exitosa"
  sender = context["var"]["value"].get("EMAIL_SENDER")
  recipient = context["var"]["value"].get("EMAIL_RECIPIENT")
  password = context["var"]["value"].get("EMAIL_PASSWORD")
  body= f"La carga de datos del dia {day} en la base de datos fue realizad con éxito"

  msg = MIMEText(body)
  msg['Subject'] = subject
  msg['From'] = sender
  msg['To'] = recipient

  with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
    smtp_server.login(sender, password)
    smtp_server.sendmail(sender, recipient, msg.as_string())
  print("Mensaje enviado")


"Quede en minuto 45 del video"