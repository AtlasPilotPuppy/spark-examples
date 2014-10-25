# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import numpy as np
from pyspark.mllib.clustering import KMeans
import matplotlib.pyplot as plt

random_points = sc.textFile("/home/anant/projects/spark-examples/data/random_points.csv")
random_points.cache()
random_points.takeSample(True, 5)

# <codecell>

data = random_points.map(lambda row: row.split(",")).map(lambda row: np.array([float(row[0]), float(row[1])]))
data.take(5)

# <codecell>

model = KMeans.train(data, 6)

# <codecell>

for i , center in enumerate(model.centers):
    print i, center

# <codecell>

x = data.map(lambda row: row[0]).collect()
y = data.map(lambda row: row[1]).collect()
plt.scatter(x, y)
plt.scatter([x[0] for x in model.centers], [x[1] for x in model.centers], s=200, c="red", alpha=0.8)
plt.show()

# <codecell>


