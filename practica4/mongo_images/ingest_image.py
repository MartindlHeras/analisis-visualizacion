import pymongo
import base64

if __name__ == '__main__':

    mongodb = pymongo.MongoClient('172.17.0.2', 27017)
    with open('sample_image.jpeg', 'rb') as f:
        image = f.read()
        bytes_image = base64.b64encode(image)

    db = mongodb.mymongo
    db.clinical_record.insert_one({'client_id': 'NAME', 'xray': bytes_image})