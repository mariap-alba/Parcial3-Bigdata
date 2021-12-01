from setuptools import setup

setup(
    name="scraping",
    version="0.3",
    packages=['scraping'],
    install_requires=['requests','json2','csv23','BeautifulSoup4','boto3','datetime','pandas','numpy']
)
