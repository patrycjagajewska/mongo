import pymongo
from pymongo import MongoClient
from bson.code import Code

__author__ = 'Patrycja'

client = MongoClient()

db = client.Wrona

#lekarze ktorzy maja czynne w sobote lub w niedziele w miescie Phoenix ocenieni wyzej niz 4 (z liczba gwiazdek wieksza niz 4)
print("1 QUERY")

query1 = db.business.find({'categories': {'$in': ['Doctors']},
                           '$or': [{'hours.Saturday': {'$exists': 'true'}}, {'hours.Sunday': {'$exists':'true'}}],
                           'stars': {'$gt': 4}, 'city': 'Phoenix'}, {'name': 1, 'full_address': 1, 'hours': 1})

print("Health business in Phoenix open on weekends with more than 4 stars grade:\n")
for item in query1:
    print(str(item['name']) + '\n' + str(item['full_address']) + '\n' + str(item['hours']))
    print('\n')


#dla kazdego stanu miasto z najmniejsza i najwieksza liczba recenzji (nazwa i liczba)
print("2 QUERY")

pipeline = [{'$group': {'_id': {'state': '$state', 'city': '$city'}, 'reviews': {'$sum': '$review_count'}}},
            {'$sort': {'reviews': 1}},
            {'$group': {'_id': '$_id.state', 'smallest_city': {'$first': '$_id.city'}, 'smallest_reviews_count': {'$first': '$reviews'},
                        'biggest_city': {'$last': '$_id.city'}, 'biggest_reviews_count': {'$last': '$reviews'}}}]

print("Cities grouped by state with biggest and smallest amount of reviews: ")
query2 = db.business.aggregate(pipeline)

for item in query2:
    print(str(item['_id']) + '\n' + '    ' + str(item['smallest_city']) + ' - ' + str(item['smallest_reviews_count']) + '\n' + '    ' + str(item['biggest_city']) + ' - ' + str(item['biggest_reviews_count']))
    print('\n')

#liczba znajomych dla kazdego z imion w bazie
print("3 QUERY")

mapF = Code(
    'function() {'
    '   emit(this.name, this.friends.length);'
    '};'
)

reduceF = Code(
    'function(name, friends) {'
    '   return Array.sum(friends);'
    '};'
)

print("Friends' count for every name in db (first 10 names): ")
result = db.user.map_reduce(mapF, reduceF, "result").find()
for item in result.limit(10):
    print(str(item["_id"])+ ", " + str(item["value"]))
