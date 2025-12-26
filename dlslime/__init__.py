from ._slime_c import *
import os


def get_cmake_dir():
    return os.path.join(os.path.dirname(__file__), "share", "cmake", "dlslime")
