from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):

    # mrjobs steps
    def steps(self):
        return [
            # chaining the steps together, the second one takes the output of the first one
            # we map the output from the given file to get all movie IDs and all ratings they have
            # then we reduce it to sum all the instances in which a movie was rated
            # then we reduce the sorted results and print them
            MRStep( mapper=self.mapper_get_movies,
                reducer=self.reducer_count_ratings),
            MRStep( reducer=self.reducer_sort_output)
        ]

    # this function maps the data file received
    def mapper_get_movies(self, _, line):
        # the rows will be split by the tabs in between them
        (userID, movieID, rating, timestamp) = line.split('\t')
        # we yield the movie id as the key and set 1 as the value
        yield movieID, 1
    
    # count the instances where movies received a rating
    def reducer_count_ratings(self, movieID, values):
        # sum all values an turn these into a string
        # then pass in the movieID
        yield str(sum(values)).zfill(4), movieID
    
    # print the movie id with the count of ratings sorted
    def reducer_sort_output(self, ratingsCount, movies):
        # loop through each movie
        for movie in movies:
             # print the movie with the amount of ratings
            yield 'Movie ' + movie, ratingsCount + ' ratings.'

if __name__ == '__main__':
    # run the code above
    RatingsBreakdown.run()
