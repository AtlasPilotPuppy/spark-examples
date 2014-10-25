# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark.mllib.recommendation import ALS

# <codecell>

def parseRating(line):
    """
    Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
    """
    fields = line.strip().split("::")
    return long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

# <codecell>

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

# <codecell>

def loadRatings(ratingsFile):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print "File %s does not exist." % ratingsFile
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print "No ratings provided."
        sys.exit(1)
    else:
        return ratings

# <codecell>

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

# <codecell>

# load personal ratings
user_ratings_file = "/home/anant/projects/spark-examples/personalRatings.txt"
myRatings = loadRatings(user_ratings_file)
myRatingsRDD = sc.parallelize(myRatings)

# <codecell>

# load ratings and movie titles
# ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
ratings_file = "/home/anant/projects/spark-examples/data/movielens/ratings.dat"
ratings = sc.textFile(ratings_file).map(parseRating)

# <codecell>

# movies is an RDD of (movieId, movieTitle)
movies_file = "/home/anant/projects/spark-examples/data/movielens/movies.dat"
movies = dict(sc.textFile(movies_file).map(parseMovie).collect())

# <codecell>

numRatings = ratings.count()
numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
numMovies = ratings.values().map(lambda r: r[1]).distinct().count()
print "Number of ratings: {}\nNumber of users: {}\nNumber of movies: {}".format(numRatings, numUsers, numMovies)

# <codecell>

numPartitions = 6
training = ratings.filter(lambda x: x[0] < 6)\
  .values().union(myRatingsRDD).repartition(numPartitions).cache()
training.take(5)

# <codecell>

validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8)\
  .values().repartition(numPartitions).cache()
validation.take(5)

# <codecell>

test = ratings.filter(lambda x: x[0] >= 8).values().cache()

# <codecell>

numTraining = training.count()
numValidation = validation.count()
numTest = test.count()
print "Training: {}, validation: {}, test: {}".format(numTraining, numValidation, numTest)

# <codecell>

# train models and evaluate them on the validation set

ranks = [8, 12]
lambdas = [0.1, 10.0]
numIters = [10, 20]
bestModel = None
bestValidationRmse = float("inf")
bestRank = 0
bestLambda = -1.0
bestNumIter = -1

for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    model = ALS.train(training, rank, numIter, lmbda)
    validationRmse = computeRmse(model, validation, numValidation)
    print ("RMSE (validation) = {} for the model trained with ".format(validationRmse),
        "rank = {}, lambda = {}, and numIter = {}.".format(rank, lmbda, numIter))
    if (validationRmse < bestValidationRmse):
        bestModel = model
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lmbda
        bestNumIter = numIter

# <codecell>


testRmse = computeRmse(bestModel, test, numTest)
# evaluate the best model on the test set
print """The best model was trained with rank = {} and lambda = {},
    and numIter = {}, and its RMSE on the test set is {}.""".format(bestRank, bestLambda, bestNumIter, testRmse)

# <codecell>

meanRating = training.union(validation).map(lambda x: x[2]).mean()
baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
improvement = (baselineRmse - testRmse) / baselineRmse * 100
print "The best model improves the baseline by {} %".format(improvement)

# <codecell>

myRatedMovieIds = set([x[1] for x in myRatings])
candidates = sc.parallelize([m for m in movies if m not in myRatedMovieIds])
predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()
recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]
print "Movies recommended for you:"
for i in xrange(len(recommendations)):
    print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

# <codecell>


