from mrjob.job import MRJob
from mrjob.step import MRStep

class MostVisitedPage(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_page,
                   reducer=self.reducer_count_visits),
            MRStep(reducer=self.reducer_find_most_visited)
        ]

    def mapper_get_page(self, _, line):
        """Extracts page views from the log file."""
        parts = line.strip().split(',')
        if len(parts) >= 4 and parts[2] == "view_page":
            page = parts[3]
            yield page, 1

    def reducer_count_visits(self, page, counts):
        """Aggregates page visit counts."""
        yield None, (sum(counts), page)

    def reducer_find_most_visited(self, _, page_counts):
        """Finds the most visited page."""
        most_visited = max(page_counts, key=lambda x: x[0])  # (count, page)
        yield most_visited[1], most_visited[0]

if __name__ == '__main__':
    MostVisitedPage.run()
