# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# <codecell>

data = MLUtils.loadLibSVMFile(sc, '/home/anant/projects/spark-examples/data/sample_libsvm_data.txt').cache()

# <codecell>

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=100)

# <codecell>

# Evaluate model on training instances and compute training error
predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
trainMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(data.count())
print('Training Mean Squared Error = ' + str(trainMSE))
print('Learned regression tree model:')
print(model)

# <codecell>


