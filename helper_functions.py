import os
import sys


def get_asset_path(filename):
    """
    This function wraps a directory or folder so that pyinstaller can access the contents
    """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, 'assets', filename)
    return os.path.join('assets', filename)
