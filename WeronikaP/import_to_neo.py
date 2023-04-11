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
        return tx.run("MATCH (n) DETACH DELETE n")

    @query
    def add_countries(self, tx, data):
        for country in data:
            tx.run("MERGE (c:Country {name: $name, geometry: $geo})",
               name=country['NAME'], geo=country['geometry'])
            # self._match_years(tx, year=country['year'])
    
    @query
    def add_years(self, tx, year):
        tx.run("MERGE (y:Year {name: $name})",
               name=year)
        self._match_years(tx, year=year)
            
    @query
    def add_geometry(self, tx, data):
        for geo in data:
            tx.run("MERGE (g:Geometry {name: $name, year: $year})",
               name=geo['geometry'], year=geo['year'])
            self._match_geometry(tx, geo['geometry'], geo['NAME'])
    
    @query
    def delete_attributes(self, tx):
        tx.run("MATCH (c:Country) REMOVE c.geometry")
        tx.run("MATCH (g:Geometry) REMOVE g.year")

    @staticmethod
    def _match_geometry(tx, geo, country):
        return tx.run(f"""MATCH (country:Country), (geo:Geometry) where geo.name = "{geo}" and country.name = "{country}"
                          MERGE (country)-[:HAS_GEOMETRY]->(geo)""")
   
    @staticmethod
    def _match_years(tx, year):
        return tx.run(f"""MATCH (geo:Geometry), (year:Year) where year.name = {year} and geo.year = {year}
                          MERGE (geo)-[:IN_YEAR]->(year)""")
   
    


    
    
