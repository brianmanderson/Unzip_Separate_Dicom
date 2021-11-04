import os
import pydicom
import time
import zipfile, os, tarfile
from threading import Thread
from multiprocessing import cpu_count
from queue import *


def read_dicom_header(path, file, dicom_dictionary):
    ds = pydicom.read_file(os.path.join(path, file), stop_before_pixels=True)
    series_description = ds.SeriesDescription
    for item in [series_description]:
        if item not in dicom_dictionary:
            dicom_dictionary[item] = []
        dicom_dictionary[item].append(file)


def dicom_reader_worker(A):
    q, dicom_dictionary = A
    while True:
        item = q.get()
        if item is None:
            break
        else:
            dicom_path, dicom_file = item
            read_dicom_header(path=dicom_path, file=dicom_file, dicom_dictionary=dicom_dictionary)
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
        thread_count = int(cpu_count()*.5)
        q = Queue(maxsize=int(thread_count))
        A = (q, dicom_dictionary)
        threads = []
        for worker in range(thread_count):
            t = Thread(target=dicom_reader_worker, args=(A,))
            t.start()
            threads.append(t)
        for dicom_file in dicom_files:
            q.put([dicom_path, dicom_file])
        for i in range(thread_count):
            q.put(None)
        for t in threads:
            t.join()
        for series_description in dicom_dictionary:
            out_path = os.path.join(dicom_path, series_description).replace(':', '').replace('>', '')
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            for dicom_file in dicom_dictionary[series_description]:
                os.rename(os.path.join(dicom_path, dicom_file), os.path.join(out_path, dicom_file))
            fid = open(os.path.join(out_path, 'Separated.txt'), 'w+')
            fid.close()
    return None


def main():
    path = r'\\ucsdhc-varis2\radonc$\00plans\Separate'
    while True:
        print('Waiting...')
        time.sleep(3)
        zip_files = [i for i in os.listdir(path) if i.endswith('.zip')]
        """
        This will unzip, rename the folder, and separate them into unique folders
        """
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
        for root, folders, files in os.walk(path):
            dicom_files = [os.path.join(root, i) for i in files if i.endswith('.dcm')]
            if dicom_files:
                # Check to make sure all of the files are transferred over
                time.sleep(5)
                while len(os.listdir(root)) != len(files):
                    time.sleep(5)
                    files = os.listdir(root)
                dicom_files = [os.path.join(root, i) for i in files if i.endswith('.dcm')]
                if 'NewFrameOfRef.txt' in files:
                    continue
                if 'Separated.txt' not in files:
                    separate_into_series(root)  # Separate into their respective exams
                    continue
                new_frame_of_ref = pydicom.uid.generate_uid()
                for dicom_file in dicom_files:
                    ds = pydicom.read_file(dicom_file)
                    ds.FrameOfReferenceUID = new_frame_of_ref
                    pydicom.write_file(filename=dicom_file, dataset=ds)
                fid = open(os.path.join(root, 'NewFrameOfRef.txt'), 'w+')
                fid.close()


if __name__ == '__main__':
    main()
