from mrjob.job import MRJob
from mrjob.step import MRStep

class MostVisitedPage(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_page,
                   reducer=self.reducer_count_visits)
        ]

    def mapper_get_page(self, _, line):
        # Assuming log file has page URLs as the last column
        # Adjust this as per the format of your log file
        fields = line.split()  # Split line into fields based on whitespace
        if len(fields) > 0:
            page = fields[-1]  # Assuming the last field is the page URL
            yield page, 1

    def reducer_count_visits(self, page, counts):
        yield None, (sum(counts), page)

    def mapper_final(self):
        pass

    def reducer_final(self):
        # Find the page with the maximum count
        max_count = 0
        most_visited_page = None
        for count, page in self.reducer_count_visits():
            if count > max_count:
                max_count = count
                most_visited_page = page
        yield most_visited_page, max_count


if __name__ == '__main__':
    MostVisitedPage.run()
