import boto3
import json
import shortuuid

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
MovieTable = dynamodb.Table('Movie')

client = boto3.client('dynamodb', region_name='ap-southeast-1')

def create_table_movie():
    table = dynamodb.create_table(
        TableName='Movie',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',  # Name of the attribute
                'AttributeType': 'S'   # S = String
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def write_to_movie(title, director):
    response = MovieTable.put_item(
        Item={
            'id': shortuuid.uuid(), # Tự sinh ID ngẫu nhiên
            'title': title,
            'director': director,
            'upvotes': 0
        }
    )
    return response

def write_to_movie_sql(data):
    data["id"] = shortuuid.uuid() # Tự sinh ID ngẫu nhiên
    params  = json.dumps(data).replace('"', "'")
    stmt = "INSERT INTO Movie value " + params
    response = client.execute_statement(Statement=stmt)
    return response


def read_from_movie(id):
    response = MovieTable.get_item(
        Key={
            'id': id
        },
        AttributesToGet=[
            'title', 'director'  # valid types dont throw error,
        ]                      # Other types should be converted to python type before sending as json response
    )
    return response

def read_from_movie_sql(id):
    stmt = "SELECT * FROM Movie WHERE id = '" + id + "'"
    response = client.execute_statement(Statement=stmt)
    return response


def update_in_movie(id, data: dict):
    response = MovieTable.update_item(
        Key={
            'id': id
        },
        AttributeUpdates={
            'title': {
                'Value': data['title'],
                # available options = DELETE(delete), PUT(set/update), ADD(increment)
                'Action': 'PUT'
            },
            'director': {
                'Value': data['director'],
                'Action': 'PUT'
            }
        },
        ReturnValues="UPDATED_NEW"  # returns the new updated values
    )
    return response


def upvote_a_movie(id):
    response = MovieTable.update_item(
        Key={
            'id': id
        },
        AttributeUpdates={
            'upvotes': {
                'Value': 1,
                'Action': 'ADD'
            }
        },
        ReturnValues="UPDATED_NEW"
    )
    response['Attributes']['upvotes'] = int(response['Attributes']['upvotes'])
    return response


def modify_director_for_movie(id, director):
    response = MovieTable.update_item(
        Key={
            'id': id
        },
        UpdateExpression='SET info.director = :director',  # set director to new value
        # ConditionExpression = '', # execute until this condition fails # no condition
        ExpressionAttributeValues={  # Value for the variables used in the above expressions
            ':new_director': director
        },
        ReturnValues="UPDATED_NEW"  # what to return
    )
    return response


def delete_from_movie(id):
    response = MovieTable.delete_item(
        Key={
            'id': id
        }
    )
    return response
