import requests
from bs4 import BeautifulSoup
import boto3
from datetime import datetime

rT = requests.get('https://www.eltiempo.com/')
soupT = BeautifulSoup(rT.text, 'html.parser')
entradasT = soupT.find_all('div', {'class': 'article-details'})
fileNewsT = open('/tmp/newsElTiempoText.txt', 'w')
fileNewsT.write(str(entradasT))

rP = requests.get('https://www.publimetro.co/')
soupP = BeautifulSoup(rP.text, 'html.parser')
entradasP = soupP.find_all('article', {'class': 'top-table-list-small-promo'})
fileNewsP = open('/tmp/newsPublimetroText.txt', 'w')
fileNewsP.write(str(entradasP))

currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year

s3 = boto3.client('s3')
with open("/tmp/newsElTiempoText.txt", "rb") as f:
    s3.upload_fileobj(f, 'newspaper-glue-raw', f'headline/raw/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day={currentDay}/newsElTiempoText.txt')

with open("/tmp/newsPublimetroText.txt", "rb") as f:
    s3.upload_fileobj(f, 'newspaper-glue-raw', f'headline/raw/periodico=Publimetro/year={currentYear}/month={currentMonth}/day={currentDay}/newsPublimetroText.txt')

