from flask import Flask, request

import controller
import converter

app = Flask(__name__)

@app.route("/hello")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/createtable')
def create_table():
    controller.create_table_movie()
    return 'Table created'


@app.route('/movie', methods=['POST'])
def add_movie():
    data = request.get_json()
    response = controller.write_to_movie(
        data['title'], data['director'])
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/movie2', methods=['POST'])
def add_movie2():
    data = request.get_json()
    response = controller.write_to_movie_sql(data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }


@app.route('/movie/<id>', methods=['GET'])
def get_movie(id):
    response = controller.read_from_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Item' in response):
            return {'Item': response['Item']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }

@app.route('/movie2/<id>', methods=['GET'])
def get_movie2(id):
    response = controller.read_from_movie_sql(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):            
            result = converter.dynamo_obj_to_python_obj(response["Items"][0])
            return result
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/movie/<int:id>', methods=['DELETE'])
def delete_movie(id):
    response = controller.delete_from_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Deleted successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }


@app.route('/movie/<id>', methods=['PUT'])
def update_movie(id):
    data = request.get_json()
    response = controller.update_in_movie(id, data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Updated successfully',
            'ModifiedAttributes': response['Attributes'],
            'response': response['ResponseMetadata']
        }
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/upvote/movie/<id>', methods=['POST'])
def upvote_movie(id):
    response = controller.upvote_a_movie(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Upvotes the movie successfully',
            'Upvotes': response['Attributes']['upvotes'],
            'response': response['ResponseMetadata']
        }
    return {
        'msg': 'Some error occured',
        'response': response
    }


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
