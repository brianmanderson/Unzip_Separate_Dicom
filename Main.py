import os
import pydicom
import time
import zipfile, os, tarfile


def read_dicom_header(path, file, dicom_dictionary):
    ds = pydicom.read_file(os.path.join(path, file), stop_before_pixels=True)
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


def rename_folder(base_path, dicom_path):
    dicom_files = [i for i in os.listdir(dicom_path) if i.endswith('.dcm')]
    for file in dicom_files:
        ds = pydicom.read_file(os.path.join(dicom_path, file))
        given_name = ds.PatientName.given_name
        family_name = ds.PatientName.family_name
        new_path = os.path.join(base_path, family_name + '_' + given_name)
        os.rename(dicom_path, new_path)
        return new_path
    return dicom_path


def separate_into_series(dicom_path):
    dicom_files = [i for i in os.listdir(dicom_path) if i.endswith('.dcm')]
    if dicom_files:
        dicom_dictionary = dict()
        for dicom_file in dicom_files:
            read_dicom_header(path=dicom_path, file=dicom_file, dicom_dictionary=dicom_dictionary)
        for series_description in dicom_dictionary:
            out_path = os.path.join(dicom_path, series_description).replace(':', '').replace('>', '')
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            for dicom_file in dicom_dictionary[series_description]:
                os.rename(os.path.join(dicom_path, dicom_file), os.path.join(out_path, dicom_file))
    return None


def main():
    path = r'\\ucsdhc-varis2\radonc$\00plans\LifeImage Exports'
    while True:
        print('Waiting...')
        time.sleep(3)
        zip_files = [i for i in os.listdir(path) if i.endswith('.zip')]
        for zip_file in zip_files:
            print('Files found...')
            time.sleep(10)
            print('Running...')
            output_path = os.path.join(path, zip_file[:-4])
            if not os.path.exists(output_path):
                unzip_file(file_path=os.path.join(path, zip_file), output_path=output_path)
                os.remove(os.path.join(path, zip_file))  # Delete zipped file
            new_path = rename_folder(base_path=path, dicom_path=output_path)
            # separate_into_series(new_path)  # Separate into their respective exams


if __name__ == '__main__':
    main()
