from DBManager import DBManager
from src.location_properties_pkg import LocationHandler

class TransactionHandler:
    # Group transactions with respect to product id's
    def groupTransactions(self):
        dbManager = DBManager()
        transactions = dbManager.select_all('transactions', 'us-east-1') 

        covered_ids = []
        group = {}
        
        for tx in transactions:
            # if this id was covered before, skip iteration
            if covered_ids.count(tx['product_id']) == 0:
                
                covered_ids.append(tx['product_id'])
                # group all the transactions with this id
                for tx2 in transactions:
                    if tx2['product_id'] == tx['product_id']:
                        try:
                            group[tx['product_id']].append({
                                'latitude': tx2['latitude'],
                                'longitude': tx2['longitude']
                            })
                        except KeyError:
                            group[tx['product_id']] = []
                            group[tx['product_id']].append({
                                'latitude': tx2['latitude'],
                                'longitude': tx2['longitude']
                            })
     
        avg_group = {}
        # iterate in the grouped transactions
        for key in group:
            count = 0
            lat = 0
            lon = 0
            # iterate in each group to sum lat and lon values
            for coord in group[key]:
               count += 1
               lat += float(coord['latitude'])
               lon += float(coord['longitude'])
            # store the average value of lat and lon values of each group
            avg_group[key] = {
                'latitude': lat/count,
                'longitude': lon/count,
            }
        
        return avg_group
        
    # Sort the grouped transactions with respect to the user's location
    def sortGroupedTransactionCoordinates(self, lat, lon, coords):
        locationHandler = LocationHandler.LocationHandler()
        sorted_coords = {}
        # get distance between the user and each group and store in a dict
        for key in coords:
            distance = locationHandler.calculateDistanceInKM(lat, lon, coords[key]['latitude'], coords[key]['longitude'])
            sorted_coords[key] = distance
        # sort the dict with respect to the distance
        sorted_coords = dict(sorted(sorted_coords.items(), key=lambda x:x[1]))
        return sorted_coords.keys()
        
    # Prepare a response for sorting the products based on location
    def prepareSortedProductsResponse(self, sorted_ids):
        dbManager = DBManager()
        response_object = []
        
        products = dbManager.select_all('products','us-east-1')
        # get each product from database based on provided id and attach it to response object
        for id in sorted_ids:
            key_info={
                "id": id
            }
            product = dbManager.get_an_item('us-east-1', 'products', key_info)
            
            response_object.append(product)
        
        # in case there are some products that were never bought before
        difference = [i for i in response_object if i not in products] \
      + [j for j in products if j not in response_object]
        response_object.extend(difference)
        
        return response_object
            