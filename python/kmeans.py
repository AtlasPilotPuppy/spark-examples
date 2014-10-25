# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import numpy as np
from pyspark.mllib.clustering import KMeans
import matplotlib.pyplot as plt

gpa_data = sc.textFile("../data/student.txt")
gpa_data.cache()
gpa_data.take(5)

# <codecell>

data = gpa_data.map(lambda row: row.split("\t")).map(lambda row: np.array([row[1], row[2]]))
data.take(5)

# <codecell>

model = KMeans.train(data, 4)

# <codecell>

model.centers

# <codecell>

model.clusterCenters

# <codecell>

x = data.map(lambda row: row[0]).collect()
y = data.map(lambda row: row[1]).collect()
plt.scatter(x, y)
plt.scatter([x[0] for x in model.centers], [x[1] for x in model.centers], s=90, c="red")
plt.show()

# <codecell>


