from mrjob.job import MRJob
from mrjob.step import MRStep

class ETLProcess(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract,
                   reducer=self.reducer_transform_count),
            MRStep(reducer=self.reducer_find_top_page)
        ]

    def mapper_extract(self, _, line):
        """Extracts only 'view_page' actions from the log file"""
        parts = line.strip().split(',')
        if len(parts) >= 4 and parts[2] == "view_page":  # Check if action is 'view_page'
            page = parts[3]  # Extract page URL
            yield page, 1  # Send page with count 1

    def reducer_transform_count(self, page, counts):
        """Counts how many times each page is visited"""
        yield None, (sum(counts), page)

    def reducer_find_top_page(self, _, page_counts):
        """Finds the most visited page"""
        most_visited = max(page_counts, key=lambda x: x[0])  # Get max count
        yield "Most Visited Page", most_visited

if __name__ == '__main__':
    ETLProcess.run()
