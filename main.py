from pymongo import MongoClient

client = MongoClient('localhost')

db = client['video']

col = db['movieDetails']

print(client.list_database_names())
print(db.list_collection_names())

print(
    "Para el rango de años superiores a 2002, ¿cuántos grupos existen en los que se pueden clasificar rated, las películas y de esas clasificaciones, la PG-13, ¿cuántas nominaciones a premios “awards nominations” ha tenido?")
for firstConsult in col.aggregate([{"$match": {
    "rated": "PG-13",
    "year": {"$gt": 2002}}},
    {"$group": {
        "_id": "PG-13",
        "sum": {"$sum": "$awards.nominations"}}}]):
    print(firstConsult)

print("")
# Consulta 2
print("Cuántas películas se filmaron en cinco países , entre los que se encuentran: USA, Serbia y China: ",
      col.count_documents({
          "countries": {
              "$size": 5,
              "$all": ["USA", "Serbia", "China"]
          }
      }))
print("")
# Consulta 3
print("Cuántas películas tienen en su título el patrón de letras ks y el nombre del director inicia con la letra D: ",
      col.count_documents({
          "title": {"$regex": "ks.*"},
          "director": {"$regex": "^D"}
      }))

print("")
print(
    "Qué películas tienen erróneamente como clasificación rated un valor de null y su valor metacrítico  es superior a 70:")
for fourConsult in col.find({
    "rated": None,
    "metacritic": {"$gt": 70}
}, {"title": 1, "_id": 0, "metacritic": 1, "rated": 1}):
    print(" ", fourConsult)

print("")
print(
    "Cuál es el máximo número de premios obtenidos  de cada una de las película de las clasificaciones rated: PG-13 y PG")
for fifthConsult in col.aggregate([{"$match": {"$or": [{"rated": "PG-13"}, {"rated": "PG"}]}},
                                   {"$group": {"_id": "$rated", "max": {"$max": "$awards.wins"}}}]):
    print(" ", fifthConsult)

print("")
print(
    "Cuántas películas han tenido más de 2 y menos de 5 premios, nominaciones  y que hayan sido producidas en el año 1958, además que el rating (imdb.rating) este entre 5 y 6:",
    col.count_documents({
        "awards.nominations": {"$gt": 2, "$lt": 5},
        "year": 1958,
        "imdb.rating": {"$lt": 6, "$gt": 5}}))
print("")
print("Qué películas, se pueden clasificar en todos los siguientes géneros(genres): Comedy, Crime,Drama")
for sevenConsult in col.find({
    "genres": {"$all": ["Comedy", "Crime", "Drama"]}
}, {"title": 1, "genres": 1, "_id": 0}):
    print(" ", sevenConsult)

print("")
print("Qué títulos de películas NO fueron grabadas en USA o India y donde hayan actuado Emily Dryden o Siân Phillips:")
for eightConsult in col.find({
    "countries": {"$nin": ["USA", "India"]},
    "actors": {"$in": ["Emily Dryden", "Siân Phillips"]}
}, {
    "title": 1,
    "countries": 1,
    "actors": 1,
    "_id": 0
}):
    print(" ", eightConsult)
