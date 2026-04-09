from appdirs import user_data_dir,user_log_dir,AppDirs
from pathlib import Path



from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("vix_utils")
except PackageNotFoundError:
    # Package is not installed (e.g., running from source without 'pip install -e .')
    __version__ = "unknown"

__override_data_dir__=None

def make_dir(p):
    return p.mkdir(exist_ok=True,parents=True) 

def override_data_dir(path:Path):
    __override_data_dir__=path

def data_dir():
    if __override_data_dir__ is None:
        dirs=AppDirs(__package__,"vixutil_co",version=f"v{__version__}")
        user_data_dir=dirs.user_data_dir
        return Path(user_data_dir)
    return __override_data_dir__
