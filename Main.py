import os
import pydicom
import time
from DicomRTTool import DicomReaderWriter
import zipfile, os, tarfile


def read_dicom_header(path, file, dicom_dictionary):
    ds = pydicom.read_file(os.path.join(path, file), stop_before_pixels=True)
    dicom_dictionary['given_name'] = ds.PatientName.given_name
    dicom_dictionary['family_name'] = ds.PatientName.family_name
    series_description = ds.SeriesDescription
    for item in [series_description]:
        if item not in dicom_dictionary:
            dicom_dictionary[item] = []
        dicom_dictionary[item].append(file)


def contour_worker(A):
    q, dicom_dictionary = A
    while True:
        file = q.get()
        if file is None:
            break
        else:
            read_dicom_header(file, dicom_dictionary)
        q.task_done()


def unzip_file(file_path, output_path):
    if file_path.find('.zip') != -1:
        zip_ref = zipfile.ZipFile(file_path, 'r')
        zip_ref.extractall(output_path)
        zip_ref.close()
    else:
        tar = tarfile.open(file_path)
        tar.extractall(output_path)
        tar.close()
    return None


path = r'\\ucsdhc-varis2\radonc$\bmanderson\unzipthings'
while True:
    time.sleep(5)
    zip_files = [i for i in os.listdir(path) if i.endswith('.zip')]
    for zip_file in zip_files:
        output_path = os.path.join(path, zip_file[:-4])
        if not os.path.exists(output_path):
            unzip_file(file_path=os.path.join(path, zip_file), output_path=output_path)
        dicom_files = [i for i in os.listdir(output_path) if i.endswith('.dcm')]
        if dicom_files:
            dicom_dictionary = dict()
            i = 0
            for dicom_file in dicom_files:
                read_dicom_header(path=output_path, file=dicom_file, dicom_dictionary=dicom_dictionary)
                i += 1
                if i % 250 == 0:
                    print(i)
            for series_description in dicom_dictionary:
                out_path = os.path.join(output_path, series_description).replace(':', '').replace('>', '')
                if not os.path.exists(out_path):
                    os.makedirs(out_path)
                for dicom_file in dicom_dictionary[series_description]:
                    os.rename(os.path.join(output_path, dicom_file), os.path.join(out_path, dicom_file))
            xxx = 1