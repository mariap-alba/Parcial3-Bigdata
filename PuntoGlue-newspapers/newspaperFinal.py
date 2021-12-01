import csv
import json
from bs4 import BeautifulSoup
import boto3
from datetime import datetime

currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year

s3 = boto3.resource('s3')

s3.meta.client.download_file('newspaper-glue-raw', f'headline/raw/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day={currentDay}/newsElTiempoText.txt', f'/tmp/newsElTiempoText.txt')

fileNewsT = open('/tmp/newsElTiempoText.txt', 'rb')
rT = fileNewsT.read()
soupT = BeautifulSoup(rT, 'html.parser')
entradasT = soupT.find_all('div', {'class': 'article-details'})

jsonNT = ["{"]
jsonNT.append('"news":[')
for i, entradaT in enumerate(entradasT):
    # Con el método "getText()" no nos devuelve el HTML
    titleT = entradaT.find('a', {'class': 'title'})
    # Sino llamamos al método "getText()" nos devuelve también el HTML
    sectionT = entradaT.find('a', {'class': 'category'})
    #fecha = entrada.find('span', {'class': 'published-at'}).getText()
    linkT = entradaT.find('a', {'class': 'title'})['href']

    if titleT != None and sectionT != None:
        titleTT = titleT.getText()
        sectionTT = sectionT.getText()
    # Imprimo el Título, Autor y Fecha de las entradas
        jsonNT.append('{"title": "'+titleTT+'", "section": "'+sectionTT +
                        '", "link": "'+'https://www.eltiempo.com' + linkT+'"},')
    #jsonN.append('{"id": "'+str(i+1)+'", "title": "'+title+'"},')
jsonNT.append("]}")

jsonNT = ''.join(jsonNT)
jsonNT = jsonNT[0: len(jsonNT)-3:] + jsonNT[len(jsonNT)-2::]
jsonNewsT = json.loads(jsonNT)

news_dataT = jsonNewsT['news']
with open("/tmp/newsElTiempo.csv", "w") as fileT:
    csv_fileT = csv.writer(fileT)
    csv_fileT.writerow(['title', 'section', 'link'])
    for itemT in news_dataT:
        csv_fileT.writerow([itemT.get('title'),itemT.get('section'),itemT.get('link')])

#---------------------------------------------------------------------------------------------------
s3.meta.client.download_file('newspaper-glue-raw', f'headline/raw/periodico=Publimetro/year={currentYear}/month={currentMonth}/day={currentDay}/newsPublimetroText.txt', f'/tmp/newsPublimetroText.txt')##cambiar en el nuevo


fileNewsP = open('/tmp/newsPublimetroText.txt', 'rb')
rP = fileNewsP.read()
soupP = BeautifulSoup(rP, 'html.parser')
entradasP = soupP.find_all('article', {'class': 'top-table-list-small-promo'})

jsonNP = ["{"]
jsonNP.append('"news":[')
for i, entradaP in enumerate(entradasP):
    titleP = entradaP.find('div', {'class': 'promo-headline'})
    sectionP = "Categoría"
    linkP = entradaP.find('div', {'class': 'promo-headline'})

    if titleP != None and linkP != None:
        titlePP = titleP.getText()
        titlePPP = titlePP.replace('"','')
        linkPP = linkP.a['href']
        sectionPP = sectionP
        jsonNP.append('{"title": "'+titlePPP+'", "section": "'+sectionPP +'", "link": "'+'https://www.publimetro.co' + linkPP+'"},')
jsonNP.append("]}")

jsonNP = ''.join(jsonNP)
jsonNP = jsonNP[0: len(jsonNP)-3:] + jsonNP[len(jsonNP)-2::]
jsonNewsP = json.loads(jsonNP)

news_dataP = jsonNewsP['news']
with open("/tmp/newsPublimetro.csv", "w") as fileP:
    csv_fileP = csv.writer(fileP)
    csv_fileP.writerow(['title', 'section', 'link'])
    for itemP in news_dataP:
        csv_fileP.writerow([itemP.get('title'),itemP.get('section'),itemP.get('link')])


s3 = boto3.client('s3')
with open('/tmp/newsElTiempo.csv', 'rb') as f:
    s3.upload_fileobj(f, 'newspaper-glue-final', f'headline/final/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day={currentDay}/newsElTiempo.csv')

with open('/tmp/newsPublimetro.csv', 'rb') as f:
    s3.upload_fileobj(f, 'newspaper-glue-final', f'headline/final/periodico=Publimetro/year={currentYear}/month={currentMonth}/day={currentDay}/newsPublimetro.csv')

