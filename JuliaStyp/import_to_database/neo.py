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
    def add_persons(self, tx, data):
        return tx.run("""UNWIND $data as n
                         MERGE (:Person {name: n})""", data=data)

    @query
    def add_places(self, tx, data):
        return tx.run("""UNWIND $data as n
                         MERGE (:Place {name: n})""", data=data)

    @query
    def add_letters(self, tx, data):
        self._create_letter(tx, data=data)
        for values in (('miejsce_do', 'SENT_TO'), ('miejsce_od', 'SENT_FROM')):
            self._match_places(tx, value=values[0], relation=values[1])
        for values in (('do', 'SENT_TO'), ('od', 'SENT_BY')):
            self._match_persons(tx, od_do=values[0], relation=values[1])

    @query
    def add_xml_info(self, tx, data):
        self._xml_info(tx, data=data)
        self._update_addressee(tx)

    @staticmethod
    def _create_letter(tx, data):
        params = data.to_dict('records')
        return tx.run("""UNWIND $data as n
                         CREATE (letter:Letter {id: n.id,
                                                osoba_od:  n.osoba_od,
                                                osoba_do: n.osoba_do,
                                                miejsce_od: n.miejsce_od,
                                                miejsce_do: n.miejsce_do,
                                                name: n.name,
                                                start: date(n.data_start),
                                                stop: date(n.data_stop),
                                                data: n.data_txt,
                                                summary: n.summary,
                                                text: n.txt})""", data=params)

    @staticmethod
    def _match_places(tx, value, relation):
        return tx.run(f"""MATCH (letter:Letter), (n:Place {{name: letter.{value}}})
                          MERGE (letter)-[:{relation}]->(n)""")

    @staticmethod
    def _match_persons(tx, od_do, relation):
        return tx.run(f"""MATCH (letter:Letter)
                          WITH letter
                          UNWIND letter.osoba_{od_do} as osoba
                              MATCH (os:Person {{name: osoba}})
                              CALL{{ 
                                    WITH letter, os 
                                    MATCH (msc:Place {{name: letter.miejsce_{od_do}}})
                                    MERGE (os)-[:WAS_IN]->(msc)
                                    }}
                              MERGE (letter)-[:{relation}]->(os)""")

    @staticmethod
    def _xml_info(tx, data):
        params = data.to_dict('records')
        xml_query = """UNWIND $data as row
                       MATCH (letter:Letter {id: row.id})\n"""
        xml_query += Neo4jDB._build_query(column='adresat', relation='-[:SENT_TO]->')
        for i in (['miejsce_wzmiankowane', 'Place'], ['osoba_prawdopodobnie_wzmiankowana'], ['osoba_wzmiankowana']):
            xml_query += Neo4jDB._build_query(*i)
        return tx.run(xml_query, data=params)

    @staticmethod
    def _build_query(column, node='Person', relation='<-[:MENTIONED_IN]-'):
        return f"""CALL{{
                         WITH letter, row
                         UNWIND row.{column} as {column}
                            MATCH (n:{node} {{name: {column}}})
                            MERGE (letter){relation}(n)
                         }}\n"""

    @staticmethod
    def _update_addressee(tx):
        return tx.run("""MATCH (letter:Letter)-[:SENT_TO]->(person:Person), (place:Place {name: letter.miejsce_do})
                         MERGE (person)-[:WAS_IN]->(place)""")
