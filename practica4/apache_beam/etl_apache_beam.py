import json
import apache_beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io.mongodbio import WriteToMongoDB

input_file = './store_tickets.json'

class JSONConverter(apache_beam.DoFn):
    def process(self, element):
        yield json.loads(element)

if __name__ == '__main__':
    with apache_beam.Pipeline(runner='DirectRunner') as pipeline:
        json_file = pipeline | ReadFromText(input_file)
        json_records = json_file | apache_beam.Map(lambda record: json.loads(record))
        json_records | WriteToMongoDB('mongodb://172.17.0.2/27017', db='mymongo', coll='store_tickets')
