import requests
from flask_restful import Api, Resource

import Configuration
from DataStorage import DataStorage
from flask import Flask, request
from json import dumps
import boto3
import datetime

db_handler = DataStorage()
app = Flask(__name__)
api = Api(app)


class Results(Resource):
    def get(self):
        self.cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
        final_result_list = []
        query_track = request.args['query']
        print('Searching for: {}'.format(query_track))
        time_before_search = datetime.datetime.now()
        db_search_results = db_handler.search_db(query_track)

        for result in db_search_results:
            edited_result = {}
            for key in result.keys():
                if key != 'content':
                    edited_result[key] = result[key]
                else:
                    edited_result['gist'] = result['content'][:100]

            final_result_list.append(edited_result)

        time_after_search = datetime.datetime.now()

        actual_time = (time_after_search - time_before_search).seconds

        instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
        self.cloudwatch.put_metric_data(
                                        MetricData=[
                                            {
                                                'MetricName': 'Search_ResponseTime',
                                                'Dimensions': [
                                                    {
                                                        'Name': 'InstanceId',
                                                        'Value': instance_id
                                                    },
                                                ],
                                                'Unit': 'None',
                                                'Value': actual_time
                                            },
                                        ],
                                        Namespace='EC2_SearchEngine'
                                    )

        print('Send the value : {} to CloadWatch'.format(actual_time))

        return {'results': final_result_list}


api.add_resource(Results, '/results')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='80')
