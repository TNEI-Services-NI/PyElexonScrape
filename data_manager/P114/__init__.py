
import os
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np

if __name__ == "__main__":
    sPAR = os.path.abspath(os.path.join(os.getcwd(), '..'))
    sCWD = os.getcwd()
    lDIR = os.listdir(sCWD)

    dfTest = pd.DataFrame({1: [0, 1, 2, 3, 4], 2: [5, 6, 7, 8, 9]})
