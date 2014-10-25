# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# <codecell>

data = MLUtils.loadLibSVMFile(sc, '../data/sample_libsvm_data.txt').cache()
data.take(5)

# <codecell>

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainClassifier(data, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=100)

# <codecell>

predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
labelsAndPredictions.take(10)

# <codecell>

trainErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(data.count())
print('Training Error = ' + str(trainErr))
print('Learned classification tree model:')
print(model)

# <codecell>


