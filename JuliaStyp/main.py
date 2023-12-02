import api_nifc
import import2neo

if __name__ == '__main__':
    api_nifc.get_all_letters()
    api_nifc.prepare_letters()
    import2neo.import_to_neo4j()
