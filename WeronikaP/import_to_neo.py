from neo4j import GraphDatabase

def query(method):
    def wrapper(*args, **kwargs):
        self = args[0]
        with self.driver.session() as session:
            session.execute_write(lambda tx: method(self, tx, **kwargs))
    return wrapper


class Neo4jDB:
    def __init__(self, uri='bolt://localhost:7687', username="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.driver.verify_connectivity()

    def close(self):
        self.driver.close()

    @query
    def clear(self, tx):
        return tx.run("""MATCH (n) DETACH DELETE n""")

    @query
    def add_countries(self, tx, data):
        for country in data:
            tx.run("""MERGE (c:Country {name: $name, geometry: $geo})""",
               name=country['NAME'], geo=country['geometry'])
    
    @query
    def add_places(self, tx, data):
        for place in data:
            tx.run("""MERGE (c:Place {name: $name, geometry: $geo})""",
               name=place['name'], geo=place['geometry'])
            
    @query
    def add_years(self, tx, year):
        tx.run("""MERGE (y:Year {name: $name})""",
               name=year)
        self._match_years(tx, year=year)
            
    @query
    def add_geometry_country(self, tx, data):
        for geo in data:
            tx.run("""MERGE (g:Geometry {name: $name, type: $type, format: $wkt, year: $year})""",
               name=geo['geometry'], type=geo['type'], wkt='WKT', year=geo['year'])
            self._match_geometry_countries_years(tx, geo['geometry'], geo['NAME'])
    
    @query
    def add_geometry_places(self, tx, data):
        for geo in data:
            tx.run("""MERGE (g:Geometry {name: $name, type: $type, format: $wkt})""",
               name=geo['geometry'], type=geo['type'], wkt='WKT')
            self._match_geometry_places(tx, geo=geo['geometry'], place=geo['name'])
    
    @query
    def delete_attributes(self, tx):
        tx.run("""MATCH (c:Country) REMOVE c.geometry""")
        tx.run("""MATCH (g:Geometry) REMOVE g.year""")
        tx.run("""MATCH (c:Place) REMOVE c.geometry""")
    
    @query
    def match_place_with_country(self, tx, place, country, year):
        for c1, c2 in zip (place, country):
            tx.run("""MATCH (place:Place {name: $c1}), (country:Country {name: $c2})
            MERGE (place)-[:IS_IN {year: $year}]->(country)""", 
            c1=c1, c2=c2, year=year)
    
    @staticmethod
    def _match_geometry_places(tx, geo, place):
        return tx.run(f"""MATCH (place:Place), (geo:Geometry) WHERE geo.name = "{geo}" and place.name = "{place}"
                          MERGE (place)-[:HAS_GEOMETRY]->(geo)""")

    @staticmethod
    def _match_geometry_countries_years(tx, geo, country):
        return tx.run(f"""MATCH (country:Country), (geo:Geometry) where geo.name = "{geo}" and country.name = "{country}"
                          MERGE (country)-[:HAS_GEOMETRY]->(geo)""")
   
    @staticmethod
    def _match_years(tx, year):
        return tx.run(f"""MATCH (geo:Geometry), (year:Year) where year.name = {year} and geo.year = {year}
                          MERGE (geo)-[:IN_YEAR]->(year)""")
   
    


    
    
