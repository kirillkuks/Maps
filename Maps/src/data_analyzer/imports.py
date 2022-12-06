import sys
import os

abs_path = os.path.abspath(__file__)[:os.path.abspath(__file__).rindex('\\')]
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_loader')
sys.path.append(abs_path[:abs_path.rindex('\\')] + '\\data_visualizer')

from cities import OverpassCityMapping, get_total_population, ECity, City
from visualizer import Visualizer
