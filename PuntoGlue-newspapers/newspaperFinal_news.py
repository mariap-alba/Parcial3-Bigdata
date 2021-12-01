import requests
import csv
import json
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3

currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year

s3 = boto3.resource('s3')

s3.meta.client.download_file('newspaper-glue-final', f'headline/final/periodico=Publimetro/year={currentYear}/month={currentMonth}/day={currentDay}/newsPublimetro.csv', f'/tmp/newsPublimetro.csv')

new=['noticias']
links=[]

with open('/tmp/newsPublimetro.csv', 'r') as File:  
    reader = csv.reader(File)
    for row in reader:
        link= row[2]
        links.append(link)
links.pop(0)
for lk in links:
    r=requests.get(lk)
    soup = BeautifulSoup(r.text, 'html.parser')
    entradas = soup.find('article')
    if entradas != None:
        entrada = entradas.getText()
        entrada = entrada.replace("Expandir","")
        new.append(entrada)
new
new = pd.DataFrame(new)
primerParte = pd.read_csv('/tmp/newsPublimetro.csv')
newsDatas= pd.concat ([primerParte, new],axis=1, ignore_index=True)
newsDatas.to_csv('/tmp/newsPublimetro.csv', sep=',', index=False) 


#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

s3.meta.client.download_file('newspaper-glue-final', f'headline/final/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day={currentDay}/newsElTiempo.csv', f'/tmp/newsElTiempo.csv')

newT=['noticias']
linksT=[]

with open('/tmp/newsElTiempo.csv', 'r') as FileT:  
    readerT = csv.reader(FileT)
    for rowT in readerT:
        linkT= rowT[2]
        linksT.append(linkT)
linksT.pop(0)
for lkT in linksT:
    if lk != '':
        rT=requests.get(lkT)
        soupT = BeautifulSoup(rT.text, 'html.parser')
        entradasT = soupT.find('div', {'class': 'articulo-contenido'})
        if entradasT != None:
            entradaT = entradasT.getText()
            newT.append(entradaT)
newT = pd.DataFrame(newT)
primerParteT = pd.read_csv('/tmp/newsElTiempo.csv')
newsDatasT= pd.concat ([primerParteT, newT],axis=1, ignore_index=True)
newsDatasT.to_csv('/tmp/newsElTiempo.csv', sep=',', index=False) 


s3 = boto3.client('s3')

with open('/tmp/newsPublimetro.csv', 'rb') as f:
    s3.upload_fileobj(f, 'newspaper-glue-final-news', f'news/raw/periodico=Publimetro/year={currentYear}/month={currentMonth}/day={currentDay}/newsPublimetro.csv')

with open('/tmp/newsElTiempo.csv', 'rb') as f:
    s3.upload_fileobj(f, 'newspaper-glue-final-news', f'news/raw/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day={currentDay}/newsElTiempo.csv')
