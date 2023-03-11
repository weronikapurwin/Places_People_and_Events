import spacy
from neo4j import GraphDatabase
import time
#import geopandas as gp
#import pandas as pd
#start_time = time.perf_counter()

#wczytac osoby i miejsca do bazy, narazie polaczyc to po numerze listu

#polaczenie z baza neo4j
driver = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j", "chopin123"))
session = driver.session()

#czyszczerzenie bazy
query_delete = "MATCH (n) DETACH DELETE n;"
session.run(query_delete)

file1 = open('Fch_ang_googleTran.txt', encoding="utf8")
lines = file1.readlines()

for i in range(0, len(lines)):
    lines[i] = lines[i].replace("\n", "")
    lines[i] = lines[i].replace('"',"")
    lines[i] = lines[i].replace("'", "")

nlp = spacy.load("en_core_web_lg")
all_data_dataType = []
# for i in range(0, len(lines)):
for i in range(0, 10):  # zeby ograniczyc czas wykonywania liczy tylko czesc
    print("################")
    string = lines[i] #string to pojedynczy list
    print(string)
    nlp = spacy.load("en_core_web_lg")
    doc = nlp(string)
    print([(X.text, X.label_) for X in doc.ents])

    data = [(X.text) for X in doc.ents]
    data_type = [(X.label_) for X in doc.ents]

    print(data)
    #print(data_type)

    data_dataType = []
    for j in range(0, len(data)):
        data_dataType.append([data[j], data_type[j]]) #tablica dwuwymiarowa wszystkich polaczen w danym liscie

    # for i in data_dataType:
    #     print(i)
    print(data_dataType)

    all_data_dataType.append(data_dataType) #tablica trojwymiarowa wszystkich polaczen we wszsytkich listach

    # dodanie danych z jednego listu
    for k in range(0, len(data)):
        if data_type[k] == 'PERSON':
            query_osoba = 'CREATE (:Osoba {nazwa:"' + str(data[k]) + '",nrlistu:"' + str(i+1) + '"})'
            session.run(query_osoba)
        elif data_type[k] == 'DATE':
            #uzyc .split do rozdzielenia roku
            query_data = 'CREATE (:Data {nazwa:"' + str(data[k]) + '",nrlistu:"' + str(i+1) + '"})'
            session.run(query_data)

    #dodanie listu
    query_list = "CREATE (:List {nrlistu:'" + str(i+1) + "',tresc:'" + str(string) + "'})"
    session.run(query_list)


#print(all_data_dataType)

# utworzenie krawedzi miedzy osobami i datami
# query_lists = "MATCH (o:Osoba), (d:Data) " \
#                 "WHERE o.nrlistu = d.nrlistu " \
#                 "CREATE (o)-[k:W_LISCIE]->(d) " \
#                 "RETURN o,k,d;"

# utworzenie krawedzi miedzy listami i osobami
query_per_list = "MATCH (o:Osoba), (l:List) " \
                "WHERE o.nrlistu = l.nrlistu " \
                "CREATE (o)-[k:W_LISCIE]->(l) " \
                "RETURN o,k,l;"

session.run(query_per_list)

# utworzenie krawedzi miedzy listami i datami
query_data_list = "MATCH (d:Data), (l:List) " \
                "WHERE d.nrlistu = l.nrlistu " \
                "CREATE (l)-[h:CZAS]->(d) " \
                "RETURN d,h,l;"

session.run(query_data_list)


# zapytanie o wyszukanie takiej samej daty i zwrocenie numerow listow w ktorej jest
query1 = "MATCH (l:List)-[r:CZAS]->(d:Data) WHERE d.nazwa = 'Saturday' RETURN l.nrlistu;"
dane1 = session.run(query1).data()
print(dane1)


# zapytanie o wyszukanie takiej samej osoby i zwrocenie numerow listow w ktorej jest
query2 = "MATCH (o:Osoba)-[f:W_LISCIE]->(l:List) WHERE o.nazwa = 'Dziewanowska' RETURN l.nrlistu"
dane2 = session.run(query2).data()
print(dane2)


session.close()
