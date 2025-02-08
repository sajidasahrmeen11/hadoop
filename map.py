from mrjob.job import MRJob

class WordCount(MRJob):

    def mapper(self, _, line):
        """Splits a line into words and emits (word, 1)."""
        for word in line.split():
            yield word.lower(), 1

    def reducer(self, word, counts):
        """Sums up counts for each word."""
        yield word, sum(counts)

if __name__ == '__main__':
    WordCount.run()
