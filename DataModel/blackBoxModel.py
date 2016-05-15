import numpy as np
from sklearn.linear_model import LinearRegression

blackBox = LinearRegression(normalize=True)
blackBox.fit(X, y)