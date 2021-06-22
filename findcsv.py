import os
import shutil


def find_and_rename_csv(location):
    for file in os.listdir(f"{location}/"):
        if file.endswith(".csv"):
            old_name = os.path.join(f"{location}/", file)
            new_name = f"{location}/prueba1.csv"
            os.rename(old_name, new_name)
            return new_name


def remove_csv(location):
    shutil.rmtree(location)
