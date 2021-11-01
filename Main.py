import os
from DicomRTTool import DicomReaderWriter
import pydicom
from Unzip_Tool.Unzip_Tool import Unzip_class
import time
import zipfile, os, tarfile


def unzip_file(file_path,output_path):
    if file_path.find('.zip') != -1:
        zip_ref = zipfile.ZipFile(file_path, 'r')
        zip_ref.extractall(output_path)
        zip_ref.close()
    else:
        tar = tarfile.open(file_path)
        tar.extractall(output_path)
        tar.close()
    return None


reader = DicomReaderWriter()
path = r'\\ucsdhc-varis2\radonc$\bmanderson\unzipthings'
while True:
    time.sleep(5)
    files = [i for i in os.listdir(path) if i.endswith('.zip')]
    for file in files:
        Unzip_class(path=path, file=file)