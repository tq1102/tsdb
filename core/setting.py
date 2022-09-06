import pathlib

import yaml

from .yaml_ext import magic_loader

PROJECT = pathlib.Path(__file__).resolve().parent.parent

file = PROJECT.joinpath('setting.yaml')
with open(file, "rb") as f:
    setting = yaml.load(f, Loader=magic_loader())


Setting = setting
