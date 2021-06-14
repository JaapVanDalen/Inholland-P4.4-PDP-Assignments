from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsCalculator(MRJob):

    def steps(self):
        return [
            # chaining the steps together, the second one takes the output of the first one
            # we map the output from the given file to get all movie IDs and all ratings they have
            # then we reduce it to sum all the instances in which a movie was rated
            # then we reduce the sorted results and print them
            MRStep(
                mapper=self.mapper_get_ratings, 
                combiner=self.combiner_count_ratings,
                reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_counts)
        ]
		
    # this function maps the data file received
    def mapper_get_ratings(self, _, line):
        # the rows will be split by the tabs in between them
        (userID, movieID, rating, timestamp) = line.split('\t')
        # we yield the movie id as the key and set 1 as the value
        yield movieID, 1
        
    # aggregate the values for the same keys (movieID)
    # the amount of rows will decrease drastically
    def combiner_count_ratings(self, movieID, counts):
        yield movieID, sum(counts)

    # count the  instances where movies received a rating
    def reducer_count_ratings(self, movieID, counts):
        # sum all values an turn these into a string
        # then pass in the movieID
        yield None, (sum(counts), movieID)
        
    # print the movie id with the count of ratings sorted
    def reducer_sort_counts(self, _, values):
        # loop through each movie sorted with reverse=true.
        for counted_ratings, movieID in sorted(values, reverse=True):
            # print the movie with the amount of ratings sorted
            yield (counted_ratings, int(movieID))


if __name__ == '__main__':
    # run the code above
    RatingsCalculator.run()