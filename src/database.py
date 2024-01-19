import os
import pathlib
from sqlalchemy import create_engine

path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
engine = create_engine("sqlite:///" + path)
