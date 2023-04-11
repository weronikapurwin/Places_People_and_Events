from import_to_neo import Neo4jDB
import geopandas as gpd
import pandas as pd
'''data source: http://web.archive.org/web/20080328104539/http://library.thinkquest.org:80/C006628/download.html'''

if __name__=="__main__":

    '''tables with data'''
    files = ['WeronikaP\dane\year_1783\cntry1783.shp', 'WeronikaP\dane\year_1815\cntry1815.shp', 'WeronikaP\dane\year_1880\cntry1880.shp', 'WeronikaP\dane\year_1914\cntry1914.shp','WeronikaP\dane\year_1920\cntry1920.shp', 'WeronikaP\dane\year_1938\cntry1938.shp']
    years = [1783, 1815, 1880, 1914, 1920, 1938]

    '''connection to neo'''
    neo = Neo4jDB()
    
    '''clearing Database'''
    neo.clear()

    '''adding countries'''
    temp = []
    for d in files:
        db = gpd.read_file(d)
        temp.append(db)

    '''merging dataframes'''
    countries = pd.concat(temp)

    '''dropping duplicated countries'''
    countries.drop_duplicates(subset=['NAME'], inplace=True)

    '''transforming geometry to wkt'''
    countries = gpd.GeoDataFrame(countries)
    countries = countries.to_wkt()

    '''transforming dataframe to list in order to import to neo4j'''
    countries = countries.to_dict('records')
    neo.add_countries(data= countries)
    '''end of adding countries'''

    '''adding nodes with countries from different years and creating relations between corresponding countries and years'''
    for f, y in zip(files, years):
        '''reading data from shp files'''
        df = gpd.read_file(f)
        '''adding columns with years (easier creating relathionships)'''
        df['year'] = y
        
        '''transforming geometry to wkt and adding columns with geometry type'''
        df = gpd.GeoDataFrame(df)
        geom_types = [x.geom_type for x in df.geometry]
        df['type'] = geom_types
        df = df.to_wkt()

        df = df.to_dict('records')

        '''adding geometry nodes and relathionships between corresponding country and geometry (HAS_GEOMETRY)'''
        neo.add_geometry(data=df)

        '''adding year nodes and relathionships between geometry and year (IN_YEAR)'''
        neo.add_years(year=y)

    '''removing attributes from nodes that will no longer be useful'''
    neo.delete_attributes() 
    '''closing connection to neo'''    
    neo.close()
