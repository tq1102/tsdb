import pathlib

import yaml

PROJECT = pathlib.Path(__file__).resolve().parent.parent

file = PROJECT.joinpath('setting.yaml')
with open(file, "rb") as f:
    setting = yaml.load(f, Loader=yaml.FullLoader)


Setting = setting
