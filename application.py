from flask import Flask, jsonify, request
from flask_cors import CORS
import os
from DBManager import DBManager
from TransactionHandler import TransactionHandler
import boto3
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required, JWTManager
import bcrypt
import uuid
from datetime import datetime

application = Flask(__name__)
CORS(application, resources=r'/*')
db = DBManager()
region = 'us-east-1'

# Setup the Flask-JWT-Extended extension
application.config["JWT_SECRET_KEY"] = "Cloud Platform Programming"
jwt = JWTManager(application)

@application.route("/login", methods=["POST"])
def login():
    post_data = request.get_json()
    email = post_data.get("email")
    password = post_data.get("password").encode('utf-8')
    
    key_info={
        "email": email
    }
    user = db.get_an_item(region, 'users', key_info)
    
    if user is None:
        return jsonify({"msg": "Incorrect login credentials"}), 401
    else:
        if not bcrypt.checkpw(password, bytes(user['password'])):
            return jsonify({"msg": "Incorrect login credentials"}), 401
        
    access_token = create_access_token(identity=email)
    
    return jsonify(access_token=access_token)

@application.route('/products', methods=['GET', 'POST'])
@jwt_required()
def get_products():
    response_object = {'status': 'success'}
    if request.method == 'POST':
        post_data = request.get_json()
        new_product = {
            'id': post_data.get('id'),
            'name': post_data.get('name'),
            'description': post_data.get('description'),
            'price': post_data.get('price'),
        } 

        db.store_an_item(region, 'products', new_product)
        response_object['message'] = 'Product added!'
    else:
        products = db.select_all('products', region)
        return products
    return jsonify(response_object), 200
    
@application.route('/sort-products', methods=['GET'])
@jwt_required()
def sort_products():
    th = TransactionHandler()
    
    # get user coords
    email = get_jwt_identity()
    key_info={
        "email": email
    }
    user = db.get_an_item(region, 'users', key_info)
    user_lat = float(user['latitude'])
    user_lon = float(user['longitude'])
    
    # group transactions
    grouped_tx = th.groupTransactions()
    
    # sort the grouped transactions
    sorted = th.sortGroupedTransactionCoordinates(user_lat, user_lon, grouped_tx)
    
    # prepare response object
    response = th.prepareSortedProductsResponse(sorted)
    
    return jsonify(response), 200

@application.route('/products/<id>', methods=['GET'])
@jwt_required()
def get_product_by_id(id):
    key_info={
        "id": id
    }
    product = db.get_an_item(region, 'products', key_info)
    
    if product is not None:
        print("not none")
        response = jsonify({
            'status': 'success',
            'product': product
        })
    else:
        print("none")
        response = jsonify({
            'status': 'false',
            'message': 'product not found'
        })
    print(response)
    return response

@application.route("/me", methods=["GET"])
@jwt_required()
def protected():
    # Access the identity of the current user with get_jwt_identity
    email = get_jwt_identity()
    key_info={
        "email": email
    }
    user = db.get_an_item(region, 'users', key_info)
    response = {
        'id': user['id'],
        'fullname': user['fullName'],
        'mobile': user['mobile'],
        'latitude': user['latitude'],
        'longitude': user['longitude'],
        'username': user['username'],
        'email': user['email']
    }
    return response, 200

@application.route("/transaction", methods=["POST"])
@jwt_required()
def transaction():
    post_data = request.get_json()
    print(post_data)
    user_id = post_data.get('user_id')
    latitude = post_data.get('coordinates')['latitude']
    longitude = post_data.get('coordinates')['longitude']
    cart = post_data.get('cart')
    
    transaction_ids = []
    
    for item in cart:
        tx = {
            'id': str(uuid.uuid1()),
            'created_at': str(datetime.now()),
            'product_id': item['product']['id'],
            'user_id': user_id,
            'quantity': int(item['count']),
            'total': int(item['count']) * int(item['product']['price']),
            'latitude': str(latitude),
            'longitude': str(longitude)
        }
        transaction_ids.append(tx['id'])
        db.store_an_item(region, 'transactions', tx)
        
    response = {'transactions': transaction_ids}
    return response, 200

@application.route("/update-coordinate", methods=["POST"])
@jwt_required()
def updateCoordinate():
    post_data = request.get_json()
    latitude = str(post_data.get('latitude'))
    longitude = str(post_data.get('longitude'))
    
    email = get_jwt_identity()
    key_info = {
        'email': email
    }
    
    response = db.update_item(
       table_name='users',
       region=region,
       key=key_info,
       updateExpression='SET latitude = :lat, longitude = :lon', 
       expressionAttributes={
           ':lat': latitude,
           ':lon': longitude
        }
    )
    
    return response, 200
    
    
if __name__ == '__main__':
    application.run()
    # application.run(host="0.0.0.0", port="8080")
    